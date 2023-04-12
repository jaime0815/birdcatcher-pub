//go:build WKAFKA
// +build WKAFKA

package states

import (
	"context"
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
	return nil
}

func getETCDClient(instance *InstanceInfo) (*clientv3.Client, error) {
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{instance.Addr},
		DialTimeout: time.Second * 10,

		// disable grpc logging
		Logger: zap.NewNop(),
	})
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	rootPath := instance.RootPath
	// ping etcd
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
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

func listCollections(cli clientv3.KV, basePath string) ([]string, []etcdpbv2.CollectionInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	colls, keys, err := common.ListProtoObjectsAdv(ctx, cli,
		path.Join(basePath, common.CollectionMetaPrefix),
		func(_ string, value []byte) bool {
			return true
		},
		func(info *etcdpbv2.CollectionInfo) bool {
			return true
		})

	snapshotColls, snapshotKeys, err := common.ListProtoObjectsAdv(ctx, cli,
		path.Join(basePath, "snapshots/root-coord/collection"),
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
		fmt.Printf("channel-watch key:%s, add value successfully\n", newKey)

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
		newKey := strings.ReplaceAll(key, oldChanPrefix, newChanPrefix)
		newKey = strings.Replace(newKey, newChanPrefix, oldChanPrefix, 1)

		info := &internalpb.MsgPosition{}
		err = proto.Unmarshal(kv.Value, info)
		if err != nil {
			continue
		}
		msgID := kafka.SerializeKafkaID(0)
		info.MsgID = msgID

		mv, err := proto.Marshal(info)
		if err != nil {
			return fmt.Errorf("failed to marshal segment info: %s", err.Error())
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

func resetSegmentCheckpoint(cli clientv3.KV, basePath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := cli.Get(ctx, path.Join(basePath, "datacoord-meta/s"), clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, kv := range resp.Kvs {
		info := &datapb.SegmentInfo{}
		err = proto.Unmarshal(kv.Value, info)
		if err != nil {
			continue
		}

		info.StartPosition = nil
		info.DmlPosition = nil

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
	keys, values, err := listCollections(cli, basePath)
	if err != nil {
		return err
	}

	if len(keys) != len(values) {
		return fmt.Errorf("keys size:%d is not equal value size:%d", len(keys), len(values))
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*600)
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
