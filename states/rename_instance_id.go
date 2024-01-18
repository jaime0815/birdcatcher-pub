//go:build WKAFKA
// +build WKAFKA

package states

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"maps"
	"path"
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
	fmt.Println("start to execute RenameInstanceIDCommand")
	if err := s.replacePChanPrefixWithinSchema(p.NewInstanceID); err != nil {
		return err
	}

	if err := s.resetChannelCheckPoint(p.NewInstanceID, p.MqType); err != nil {
		return err
	}

	if err := s.resetChannelWatch(p.NewInstanceID); err != nil {
		return err
	}

	if err := s.removeRemovalChannel(); err != nil {
		return err
	}

	if err := s.resetSegmentCheckpoint(p.NewInstanceID, p.MqType); err != nil {
		return err
	}

	if err := s.renameRemainedKeys(p.NewInstanceID); err != nil {
		return err
	}

	return nil
}

func listCollectionProtoObjects(ctx context.Context, kv metakv.MetaKV, prefix string) (map[string]*etcdpbv2.CollectionInfo, map[string]string, error) {
	keys, vals, err := kv.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, nil, err
	}
	if len(keys) != len(vals) {
		return nil, nil, fmt.Errorf("Error: keys and vals of different size in listCollectionProtoObjects:%d vs %d", len(keys), len(vals))
	}
	validCollections := make(map[string]*etcdpbv2.CollectionInfo)
	invalidCollections := make(map[string]string)

	for i, val := range vals {
		if bytes.Equal([]byte(val), common.CollectionTombstone) {
			invalidCollections[keys[i]] = val
			continue
		}

		var elem etcdpbv2.CollectionInfo
		err = proto.Unmarshal([]byte(val), &elem)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		validCollections[keys[i]] = &elem
	}

	return validCollections, invalidCollections, nil
}

func listCollections(cli metakv.MetaKV, basePath string, metaPath string) (map[string]*etcdpbv2.CollectionInfo, map[string]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*900)
	defer cancel()
	validCollections, invalidCollections, err := listCollectionProtoObjects(ctx, cli, path.Join(basePath, metaPath))

	snapshotValidCollections, snapshotInvalidCollections, err :=
		listCollectionProtoObjects(ctx, cli, path.Join(basePath, path.Join("snapshots", metaPath)))

	maps.Copy(validCollections, snapshotValidCollections)
	maps.Copy(invalidCollections, snapshotInvalidCollections)

	return validCollections, invalidCollections, err
}

func (s *InstanceState) resetChannelWatch(newChanPrefix string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*300)
	defer cancel()
	keys, values, err := s.client.LoadWithPrefix(ctx, path.Join(s.basePath, channelWatch))
	if err != nil {
		return err
	}

	fmt.Println("== start to update channel-watch key")
	for i, key := range keys {
		cw := &datapb.ChannelWatchInfo{}
		if err := proto.Unmarshal([]byte(values[i]), cw); err != nil {
			return err
		}

		vChannelName := s.replace(cw.Vchan.ChannelName, newChanPrefix)
		vChanInfo := &datapb.VchannelInfo{
			ChannelName:  vChannelName,
			CollectionID: cw.Vchan.CollectionID,
		}

		cw.Vchan = vChanInfo
		mv, err := proto.Marshal(cw)
		if err != nil {
			return fmt.Errorf("failed to marshal VchannelInfo: %s", err.Error())
		}

		err = s.updateKeyValue(ctx, key, newChanPrefix, string(mv))
		if err != nil {
			return err
		}
	}

	fmt.Println("== update channel-watch key count:", len(keys))
	return nil
}

func (s *InstanceState) removeRemovalChannel() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*300)
	defer cancel()
	err := s.client.RemoveWithPrefix(ctx, path.Join(s.basePath, channelRemoval))
	if err != nil {
		return err
	}

	fmt.Println("== remove channel-removal key successfully")
	return nil
}

func (s *InstanceState) resetChannelCheckPoint(newChanPrefix string, mqType string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*300)
	defer cancel()
	keys, values, err := s.client.LoadWithPrefix(ctx, path.Join(s.basePath, channelCP))
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

		s.replacePosition(info, newChanPrefix, mqType)
		mv, err := proto.Marshal(info)
		if err != nil {
			return fmt.Errorf("failed to marshal msg position info: %s", err.Error())
		}

		err = s.updateKeyValue(ctx, key, newChanPrefix, string(mv))
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

func (s *InstanceState) replacePosition(info *internalpb.MsgPosition, newChanPrefix string, mqType string) {
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
	info.ChannelName = s.replace(info.ChannelName, newChanPrefix)
}

func (s *InstanceState) replace(source, newChanPrefix string) string {
	return strings.Replace(source, s.instanceName, newChanPrefix, -1)
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

func (s *InstanceState) resetSegmentCheckpoint(newChanPrefix string, mqType string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1800)
	defer cancel()

	fmt.Println("== start to update segments key")
	count := 0
	err := s.client.WalkWithPrefix(ctx, path.Join(s.basePath, segmentPrefix), paginationSize, func(k []byte, v []byte) error {
		key := string(k)
		if strings.Contains(key, "statslog") {
			count++
			return s.updateKeyValue(ctx, key, newChanPrefix, string(v))
		}

		info := &datapb.SegmentInfo{}
		err := proto.Unmarshal(v, info)
		if err != nil {
			return err
		}

		s.replacePosition(info.StartPosition, newChanPrefix, mqType)
		s.replacePosition(info.DmlPosition, newChanPrefix, mqType)
		info.InsertChannel = s.replace(info.InsertChannel, newChanPrefix)

		mv, err := proto.Marshal(info)
		if err != nil {
			return fmt.Errorf("failed to marshal segment info: %s", err.Error())
		}

		err = s.updateKeyValue(ctx, key, newChanPrefix, string(mv))
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

func (s *InstanceState) replacePChanPrefixWithinSchema(newChanPrefix string) error {
	validCollections0, invalidCollections0, err0 := listCollections(s.client, s.basePath, common.CollectionMetaPrefix)
	if err0 != nil {
		return err0
	}

	validCollections1, invalidCollections1, err1 := listCollections(s.client, s.basePath, common.DBCollectionMetaPrefix)
	if err1 != nil {
		return err1
	}

	maps.Copy(validCollections0, validCollections1)
	maps.Copy(invalidCollections0, invalidCollections1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*900)
	defer cancel()

	fmt.Println("== update channel prefix of schema key size:", len(validCollections0)+len(invalidCollections0))
	for k, v := range validCollections0 {
		newPChannels := make([]string, 0, len(v.PhysicalChannelNames))
		newVChannels := make([]string, 0, len(v.VirtualChannelNames))

		for _, e := range v.PhysicalChannelNames {
			pchan := s.replace(e, newChanPrefix)
			newPChannels = append(newPChannels, pchan)
		}

		for _, e := range v.VirtualChannelNames {
			vchan := s.replace(e, newChanPrefix)
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
		mv, err := proto.Marshal(v)
		if err != nil {
			return fmt.Errorf("failed to marshal collection info: %s", err.Error())
		}

		err = s.updateKeyValue(ctx, k, newChanPrefix, string(mv))
		if err != nil {
			fmt.Printf("collection key:%s, update value fails\n", k)
			return err
		}
	}
	fmt.Println("== update channel prefix of valid collections:", len(validCollections0))

	for k, v := range invalidCollections0 {
		err := s.updateKeyValue(ctx, k, newChanPrefix, v)
		if err != nil {
			fmt.Printf("collection key:%s, update value fails\n", k)
			return err
		}
	}
	fmt.Println("== update channel prefix of invalid collections:", len(invalidCollections0))
	fmt.Println("replace channel prefix and reset start position successfully")
	return nil
}

func (s *InstanceState) updateKeyValue(ctx context.Context, key, newChanPrefix, value string) error {
	newKey := s.replace(key, newChanPrefix)
	err := s.client.Save(ctx, newKey, value)
	if err != nil {
		fmt.Printf("rename key:%s, update value fails\n", key)
		return err
	}

	err = s.client.Remove(ctx, key)
	if err != nil {
		fmt.Printf("delete key:%s fails\n", key)
		return err
	}
	fmt.Printf("rename key:%s to %s done\n", key, newKey)
	return nil
}

func (s *InstanceState) renameRemainedKeys(newChanPrefix string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1800)
	defer cancel()

	count := 0
	fmt.Println("== start to rename remained keys")
	err := s.client.WalkWithPrefix(ctx, s.basePath, paginationSize, func(key []byte, value []byte) error {
		k := string(key)
		if strings.Contains(k, channelWatch) ||
			strings.Contains(k, channelRemoval) ||
			strings.Contains(k, channelCP) ||
			strings.Contains(k, segmentPrefix) ||
			strings.Contains(k, common.CollectionMetaPrefix) ||
			strings.Contains(k, common.DBCollectionMetaPrefix) {
			return nil
		}

		err := s.updateKeyValue(ctx, k, newChanPrefix, string(value))
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
