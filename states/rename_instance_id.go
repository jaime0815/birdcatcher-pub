//go:build WKAFKA
// +build WKAFKA

package states

import (
	"context"
	"encoding/binary"
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/mq/kafka"
	mypulsar "github.com/milvus-io/birdwatcher/mq/pulsar"
	"github.com/milvus-io/birdwatcher/proto/v2.2/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
	etcdpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/etcdpb"
	"github.com/milvus-io/birdwatcher/proto/v2.2/internalpb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	metakv "github.com/milvus-io/birdwatcher/states/kv"
)

var (
	channelWatch   = "channelwatch"
	channelRemoval = "datacoord-meta/channel-removal"
	channelCP      = "datacoord-meta/channel-cp"
	segmentPrefix  = "datacoord-meta/s"
	paginationSize = 128
)

type RenameInstanceIDParam struct {
	framework.ParamBase `use:"renameInstanceID"`
	MqType              string `name:"mqType" default:"kafka" desc:"mq type: kafka or pulsar"`
	NewInstanceID       string `name:"newInstanceID" default:"" desc:"dest new instance id"`
}

// RenameInstanceIDCommand returns command for rename instance id of all metadata
func (s *InstanceState) RenameInstanceIDCommand(ctx context.Context, p *RenameInstanceIDParam) error {
	if err := replacePChanPrefixWithinSchema(s.client, s.basePath, p.NewInstanceID); err != nil {
		return err
	}

	if err := resetChannelCheckPoint(s.client, s.basePath, p.NewInstanceID, p.MqType); err != nil {
		return err
	}

	if err := resetChannelWatch(s.client, s.basePath, p.NewInstanceID); err != nil {
		return err
	}

	if err := removeRemovalChannel(s.client, s.basePath); err != nil {
		return err
	}

	if err := resetSegmentCheckpoint(s.client, s.basePath, p.NewInstanceID, p.MqType); err != nil {
		return err
	}

	if err := renameRemainedKeys(s.client, s.basePath, p.NewInstanceID); err != nil {
		return err
	}

	return nil
}

func listCollections(cli metakv.MetaKV, basePath string, metaPath string) ([]string, []etcdpbv2.CollectionInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*300)
	defer cancel()
	colls, keys, err := common.ListProtoObjectsAdv(ctx, cli,
		path.Join(basePath, metaPath),
		func(_ string, value []byte) bool {
			return true
		},
		func(info *etcdpbv2.CollectionInfo) bool {
			return true
		})

	snapshotColls, snapshotKeys, err := common.ListProtoObjectsAdv(ctx, cli,
		path.Join(basePath, path.Join("snapshots", metaPath)),
		func(_ string, value []byte) bool {
			return true
		},
		func(info *etcdpbv2.CollectionInfo) bool {
			return true
		})

	keys = append(keys, snapshotKeys...)
	colls = append(colls, snapshotColls...)
	return keys, colls, err
}

func resetChannelWatch(cli metakv.MetaKV, basePath string, newChanPrefix string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
	defer cancel()
	keys, values, err := cli.LoadWithPrefix(ctx, path.Join(basePath, channelWatch))
	if err != nil {
		return err
	}

	fmt.Println("== start to update channel-watch key")
	for i, key := range keys {
		cw := &datapb.ChannelWatchInfo{}
		if err := proto.Unmarshal([]byte(values[i]), cw); err != nil {
			return err
		}

		vChannelName := replace(cw.Vchan.ChannelName, newChanPrefix)
		vChanInfo := &datapb.VchannelInfo{
			ChannelName:  vChannelName,
			CollectionID: cw.Vchan.CollectionID,
		}

		cw.Vchan = vChanInfo
		mv, err := proto.Marshal(cw)
		if err != nil {
			return fmt.Errorf("failed to marshal VchannelInfo: %s", err.Error())
		}

		err = updateKeyValue(ctx, cli, key, newChanPrefix, string(mv))
		if err != nil {
			return err
		}
	}

	fmt.Println("== update channel-watch key count:", len(keys))
	return nil
}

func removeRemovalChannel(cli metakv.MetaKV, basePath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
	defer cancel()
	err := cli.RemoveWithPrefix(ctx, path.Join(basePath, channelRemoval))
	if err != nil {
		return err
	}

	fmt.Println("== remove channel-removal key successfully")
	return nil
}

func resetChannelCheckPoint(cli metakv.MetaKV, basePath string, newChanPrefix string, mqType string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
	defer cancel()
	keys, values, err := cli.LoadWithPrefix(ctx, path.Join(basePath, channelCP))
	if err != nil {
		return err
	}

	fmt.Println("== update channel-cp key count:", len(keys))
	for i, key := range keys {
		info := &internalpb.MsgPosition{}
		err = proto.Unmarshal([]byte(values[i]), info)
		if err != nil {
			continue
		}

		replacePosition(info, newChanPrefix, mqType)
		mv, err := proto.Marshal(info)
		if err != nil {
			return fmt.Errorf("failed to marshal msg position info: %s", err.Error())
		}

		err = updateKeyValue(ctx, cli, key, newChanPrefix, string(mv))
		if err != nil {
			return err
		}
		fmt.Printf("delete key:%s successfully\n", key)
	}

	return nil
}

func serializeRmqID(messageID int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(messageID))
	return b
}

func replacePosition(info *internalpb.MsgPosition, newChanPrefix string, mqType string) {
	if info == nil {
		return
	}

	switch mqType {
	case "kafka":
		info.MsgID = kafka.SerializeKafkaID(0)
	case "pulsar":
		info.MsgID = mypulsar.SerializePulsarMsgID(pulsar.EarliestMessageID())
	case "rockmq":
		info.MsgID = serializeRmqID(0)
	default:
		panic("invalid my type:" + mqType)
	}

	info.Timestamp = getCurrentTime()
	info.ChannelName = replace(info.ChannelName, newChanPrefix)
}

func replace(source, newChanPrefix string) string {
	re := regexp.MustCompile(`(?m)(in01-[0-9a-zA-Z]+)`)
	for _, match := range re.FindAllString(source, -1) {
		source = strings.ReplaceAll(source, match, newChanPrefix)
	}
	return source
}

// composeTS returns a timestamp composed of physical part and logical part
func composeTS(physical, logical int64) uint64 {
	return uint64((physical << logicalBits) + logical)
}

// composeTSByTime returns a timestamp composed of physical time.Time and logical time
func composeTSByTime(physical time.Time, logical int64) uint64 {
	return composeTS(physical.UnixNano()/int64(time.Millisecond), logical)
}

// getCurrentTime returns the future timestamp
func getCurrentTime() uint64 {
	return composeTSByTime(time.Now(), 0)
}

func resetSegmentCheckpoint(cli metakv.MetaKV, basePath string, newChanPrefix string, mqType string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*900)
	defer cancel()

	fmt.Println("== start to update segments key")
	count := 0
	err := cli.WalkWithPrefix(ctx, path.Join(basePath, segmentPrefix), paginationSize, func(k []byte, v []byte) error {
		key := string(k)
		if strings.Contains(key, "statslog") {
			count++
			return updateKeyValue(ctx, cli, key, newChanPrefix, string(v))
		}

		info := &datapb.SegmentInfo{}
		err := proto.Unmarshal(v, info)
		if err != nil {
			return err
		}

		replacePosition(info.StartPosition, newChanPrefix, mqType)
		replacePosition(info.DmlPosition, newChanPrefix, mqType)
		info.InsertChannel = replace(info.InsertChannel, newChanPrefix)

		mv, err := proto.Marshal(info)
		if err != nil {
			return fmt.Errorf("failed to marshal segment info: %s", err.Error())
		}

		err = updateKeyValue(ctx, cli, key, newChanPrefix, string(mv))
		if err != nil {
			return err
		}

		count++
		return nil
	})

	if err != nil {
		return err
	}

	fmt.Println("== update segments key successfully, count:", count)
	return nil
}

func replacePChanPrefixWithinSchema(cli metakv.MetaKV, basePath, newChanPrefix string) error {
	keys0, values0, err0 := listCollections(cli, basePath, common.CollectionMetaPrefix)
	if err0 != nil {
		return err0
	}

	keys1, values1, err1 := listCollections(cli, basePath, common.DBCollectionMetaPrefix)
	if err1 != nil {
		return err1
	}

	keys := make([]string, 0)
	keys = append(keys, keys0...)
	keys = append(keys, keys1...)

	values := make([]etcdpbv2.CollectionInfo, 0)
	values = append(values, values0...)
	values = append(values, values1...)

	if len(keys) != len(values) {
		return fmt.Errorf("keys size:%d is not equal value size:%d", len(keys), len(values))
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*300)
	defer cancel()

	fmt.Println("== update channel prefix of schema key size:", len(keys))
	for i, k := range keys {
		v := values[i]
		newPChannels := make([]string, 0, len(v.PhysicalChannelNames))
		newVChannels := make([]string, 0, len(v.VirtualChannelNames))

		for _, e := range v.PhysicalChannelNames {
			pchan := replace(e, newChanPrefix)
			newPChannels = append(newPChannels, pchan)
		}

		for _, e := range v.VirtualChannelNames {
			vchan := replace(e, newChanPrefix)
			newVChannels = append(newVChannels, vchan)
		}

		if len(newPChannels) != 0 {
			fmt.Printf("collection key:%s, old pchannels:%s replace to new pchannels:%s\n",
				k, strings.Join(v.PhysicalChannelNames, ", "), strings.Join(newPChannels, ", "))
			v.PhysicalChannelNames = newPChannels
		}

		if len(newVChannels) != 0 {
			fmt.Printf("collection key:%s, old vchannels:%s replace to new vchannels:%s\n",
				k, strings.Join(v.VirtualChannelNames, ", "), strings.Join(newVChannels, ", "))
			v.VirtualChannelNames = newVChannels
		}

		sp := make(map[string][]byte)
		for _, k := range v.PhysicalChannelNames {
			sp[k] = kafka.SerializeKafkaID(0)
		}

		v.StartPositions = toKeyDataPairs(sp)
		mv, err := proto.Marshal(&v)
		if err != nil {
			return fmt.Errorf("failed to marshal collection info: %s", err.Error())
		}

		err = updateKeyValue(ctx, cli, k, newChanPrefix, string(mv))
		if err != nil {
			fmt.Printf("collection key:%s, update value fails\n", k)
			return err
		}
	}

	fmt.Println("replace channel prefix and reset start position successfully")
	return nil
}

func updateKeyValue(ctx context.Context, cli metakv.MetaKV, key, newChanPrefix, value string) error {
	newKey := replace(key, newChanPrefix)
	err := cli.Save(ctx, newKey, value)
	if err != nil {
		fmt.Printf("rename key:%s, update value fails\n", key)
		return err
	}

	err = cli.Remove(ctx, key)
	if err != nil {
		fmt.Printf("delete key:%s fails\n", key)
		return err
	}
	fmt.Printf("rename key:%s done\n", key)
	return nil
}

func renameRemainedKeys(cli metakv.MetaKV, basePath string, newChanPrefix string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*15)
	defer cancel()

	count := 0
	fmt.Println("== start to rename remained keys")
	err := cli.WalkWithPrefix(ctx, basePath, paginationSize, func(key []byte, value []byte) error {
		k := string(key)
		if strings.Contains(k, channelWatch) ||
			strings.Contains(k, channelRemoval) ||
			strings.Contains(k, channelCP) ||
			strings.Contains(k, segmentPrefix) ||
			strings.Contains(k, common.CollectionMetaPrefix) ||
			strings.Contains(k, common.DBCollectionMetaPrefix) {
			return nil
		}

		err := updateKeyValue(ctx, cli, k, newChanPrefix, string(value))
		if err != nil {
			return err
		}

		count++
		return nil
	})

	if err != nil {
		return err
	}
	fmt.Println("rename remained keys successfully, count:", count)
	return nil
}

func toKeyDataPairs(m map[string][]byte) []*commonpb.KeyDataPair {
	ret := make([]*commonpb.KeyDataPair, 0, len(m))
	for k, data := range m {
		ret = append(ret, &commonpb.KeyDataPair{
			Key:  k,
			Data: data,
		})
	}
	return ret
}
