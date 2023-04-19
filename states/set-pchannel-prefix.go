//go:build WKAFKA
// +build WKAFKA

package states

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"path"
	"strings"
	"time"

	"github.com/milvus-io/birdwatcher/mq/kafka"
	"github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
	etcdpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/etcdpb"
	"github.com/milvus-io/birdwatcher/proto/v2.2/internalpb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

// SetChannel Only support Kafka MQ
// Usage:
// ./bin/birdwatcher --setPChanPrefix --addr 10.15.29.220:2379 --rootPath in01-6b61b7649908674 --oldPChanPrefix in01-6b61b7649908674 --newPChanPrefix  in01-fbf9815060cce85
func SetChannel(addr string, rootPath, oldPChanNamePrefix, newPChanNamePrefix string) error {
	if len(addr) == 0 {
		return errors.New("etcd address is empty")
	}
	if len(rootPath) == 0 {
		return errors.New("etcd rootpath is empty")
	}
	if len(oldPChanNamePrefix) == 0 {
		return errors.New("oldPChanPrefix is empty")
	}
	if len(newPChanNamePrefix) == 0 {
		return errors.New("newPChanPrefix is empty")
	}

	client, err := getETCDClient(&InstanceInfo{
		Addr:     addr,
		RootPath: rootPath,
	})
	if err != nil {
		return err
	}
	defer client.Close()

	fmt.Println("Using meta path:", fmt.Sprintf("%s/%s/", rootPath, metaPath))
	basePath := path.Join(rootPath, metaPath)

	if err = replacePChanPrefixWithinSchema(client, basePath, oldPChanNamePrefix, newPChanNamePrefix); err != nil {
		return err
	}

	if err = resetChannelCheckPoint(client, basePath, oldPChanNamePrefix, newPChanNamePrefix); err != nil {
		return err
	}

	if err = resetChannelWatch(client, basePath, oldPChanNamePrefix, newPChanNamePrefix); err != nil {
		return err
	}

	if err = removeRemovalChannel(client, basePath); err != nil {
		return err
	}

	if err = resetSegmentCheckpoint(client, basePath, oldPChanNamePrefix, newPChanNamePrefix); err != nil {
		return err
	}

	return nil
}

func getETCDClient(instance *InstanceInfo) (*clientv3.Client, error) {
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{instance.Addr},
		DialTimeout: time.Second * 60,

		// disable grpc logging
		Logger: zap.NewNop(),
	})
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	rootPath := instance.RootPath
	// ping etcd
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	err = pingEtcd(ctx, etcdCli, rootPath, metaPath)
	if err != nil {
		if errors.Is(err, ErrNotMilvsuRootPath) {
			etcdCli.Close()
			fmt.Printf("Connection established, but %s, please check your config or use Dry mode\n", err.Error())
			return nil, err
		}
		fmt.Println("cannot connect to etcd with addr:", instance.Addr, err.Error())
		return nil, err
	}

	return etcdCli, nil
}

func listCollections(cli clientv3.KV, basePath string, metaPath string) ([]string, []etcdpbv2.CollectionInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
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

func resetChannelWatch(cli clientv3.KV, basePath string, oldChanPrefix, newChanPrefix string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	resp, err := cli.Get(ctx, path.Join(basePath, "channelwatch"), clientv3.WithPrefix())
	if err != nil {
		return err
	}

	fmt.Println("== update channel-watch key size:", len(resp.Kvs))
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		cw := &datapb.ChannelWatchInfo{}
		if err := proto.Unmarshal(kv.Value, cw); err != nil {
			return err
		}

		newKey := strings.ReplaceAll(key, oldChanPrefix, newChanPrefix)
		newKey = strings.Replace(newKey, newChanPrefix, oldChanPrefix, 1)

		vChannelName := strings.ReplaceAll(cw.Vchan.ChannelName, oldChanPrefix, newChanPrefix)

		vChanInfo := &datapb.VchannelInfo{
			ChannelName:  vChannelName,
			CollectionID: cw.Vchan.CollectionID,
		}

		cw.Vchan = vChanInfo
		mv, err := proto.Marshal(cw)
		if err != nil {
			return fmt.Errorf("failed to marshal VchannelInfo: %s", err.Error())
		}

		// Put new KV
		_, err = cli.Put(ctx, newKey, string(mv))
		if err != nil {
			fmt.Printf("channel-watch key:%s, add value fails\n", newKey)
			return err
		}
		fmt.Printf("add channel-watch key:%s,  value successfully\n", newKey)

		// Delete old KV
		_, err = cli.Delete(ctx, key)
		if err != nil {
			fmt.Printf("delete key:%s fails\n", key)
			return err
		}
		fmt.Printf("delete key:%s successfully\n", key)
	}
	return nil
}

func removeRemovalChannel(cli clientv3.KV, basePath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	resp, err := cli.Get(ctx, path.Join(basePath, "datacoord-meta/channel-removal"), clientv3.WithPrefix())
	if err != nil {
		return err
	}
	fmt.Println("== remove channel-removal key size:", len(resp.Kvs))
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		_, err = cli.Delete(ctx, key)
		if err != nil {
			fmt.Printf("delete key:%s fails\n", key)
			return err
		}
		fmt.Printf("delete key:%s successfully\n", key)
	}

	return nil
}

func resetChannelCheckPoint(cli clientv3.KV, basePath string, oldChanPrefix, newChanPrefix string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	resp, err := cli.Get(ctx, path.Join(basePath, "datacoord-meta/channel-cp"), clientv3.WithPrefix())
	if err != nil {
		return err
	}

	fmt.Println("== update channel-cp key size:", len(resp.Kvs))
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if !strings.HasPrefix(key, oldChanPrefix) {
			fmt.Printf("channel-cp:%s has not a prefix:%s\n", key, oldChanPrefix)
			continue
		}
		newKey := strings.ReplaceAll(key, oldChanPrefix, newChanPrefix)
		newKey = strings.Replace(newKey, newChanPrefix, oldChanPrefix, 1)

		info := &internalpb.MsgPosition{}
		err = proto.Unmarshal(kv.Value, info)
		if err != nil {
			continue
		}
		replacePosition(info, oldChanPrefix, newChanPrefix)

		mv, err := proto.Marshal(info)
		if err != nil {
			return fmt.Errorf("failed to marshal msg position info: %s", err.Error())
		}

		_, err = cli.Put(ctx, newKey, string(mv))
		if err != nil {
			fmt.Printf("channel-cp key:%s, update value fails\n", key)
			return err
		}
		fmt.Println(newKey, "=== new channel-cp :", info)

		_, err = cli.Delete(ctx, key)
		if err != nil {
			fmt.Printf("delete key:%s fails\n", key)
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

func replacePosition(info *internalpb.MsgPosition, oldChanPrefix, newChanPrefix string) {
	if info == nil {
		return
	}
	//info.MsgID = serializeRmqID(0)
	info.MsgID = kafka.SerializeKafkaID(0)
	info.Timestamp = getCurrentTime()
	info.ChannelName = strings.ReplaceAll(info.ChannelName, oldChanPrefix, newChanPrefix)
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

func resetSegmentCheckpoint(cli clientv3.KV, basePath string, oldChanPrefix, newChanPrefix string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
	defer cancel()
	resp, err := cli.Get(ctx, path.Join(basePath, "datacoord-meta/s"), clientv3.WithPrefix())
	if err != nil {
		return err
	}

	fmt.Println("== update segments key size:", len(resp.Kvs))
	for _, kv := range resp.Kvs {
		info := &datapb.SegmentInfo{}
		err = proto.Unmarshal(kv.Value, info)
		if err != nil {
			continue
		}

		if !strings.HasPrefix(info.InsertChannel, oldChanPrefix) {
			fmt.Printf("segment insert channel:%s has not a prefix:%s\n", info.InsertChannel, oldChanPrefix)
			continue
		}

		replacePosition(info.StartPosition, oldChanPrefix, newChanPrefix)
		replacePosition(info.DmlPosition, oldChanPrefix, newChanPrefix)
		info.InsertChannel = strings.ReplaceAll(info.InsertChannel, oldChanPrefix, newChanPrefix)

		mv, err := proto.Marshal(info)
		if err != nil {
			return fmt.Errorf("failed to marshal segment info: %s", err.Error())
		}

		key := string(kv.Key)
		fmt.Println(key, "=== new segment :", info)
		_, err = cli.Put(ctx, key, string(mv))
		if err != nil {
			fmt.Printf("segment key:%s, update value fails\n", key)
			return err
		}
	}
	return nil
}

func replacePChanPrefixWithinSchema(cli clientv3.KV, basePath, oldChanPrefix, newChanPrefix string) error {
	keys0, values0, err0 := listCollections(cli, basePath, common.CollectionMetaPrefix)
	if err0 != nil {
		return err0
	}

	keys1, values1, err1 := listCollections(cli, basePath, common.CollectionInfoMetaPrefix)
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	fmt.Println("== update channel prefix of schema key size:", len(keys))
	for i, k := range keys {
		v := values[i]
		newPChannels := make([]string, 0, len(v.PhysicalChannelNames))
		newVChannels := make([]string, 0, len(v.VirtualChannelNames))

		for _, e := range v.PhysicalChannelNames {
			if !strings.HasPrefix(e, oldChanPrefix) {
				fmt.Printf("collection:%s, pchannel:%s has not a prefix:%s\n", v.GetSchema().Name, e, oldChanPrefix)
				continue
			}

			pchan := strings.ReplaceAll(e, oldChanPrefix, newChanPrefix)
			newPChannels = append(newPChannels, pchan)
		}

		for _, e := range v.VirtualChannelNames {
			if !strings.Contains(e, oldChanPrefix) {
				fmt.Printf("collection:%s, vchannel:%s has not a prefix:%s\n", v.GetSchema().Name, e, oldChanPrefix)
				continue
			}

			vchan := strings.ReplaceAll(e, oldChanPrefix, newChanPrefix)
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

		v.StartPositions = nil
		mv, err := proto.Marshal(&v)
		if err != nil {
			return fmt.Errorf("failed to marshal collection info: %s", err.Error())
		}

		_, err = cli.Put(ctx, k, string(mv))
		if err != nil {
			fmt.Printf("collection key:%s, update value fails\n", k)
			return err
		}
	}

	fmt.Println("replace channel prefix and reset start position successfully")
	return nil
}
