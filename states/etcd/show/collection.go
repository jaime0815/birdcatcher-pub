package show

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/utils"
)

// CollectionCommand returns sub command for showCmd.
// show collection [options...]
type CollectionParam struct {
	framework.ParamBase `use:"show collections" desc:"list current available collection from RootCoord"`
	CollectionID        int64  `name:"id" default:"0" desc:"collection id to display"`
	CollectionName      string `name:"name" default:"" desc:"collection name to display"`
	DatabaseID          int64  `name:"dbid" default:"-1" desc:"database id to filter"`
	State               string `name:"state" default:"" desc:"collection state to filter"`
}

func replace(source, old, newChanPrefix string) string {
	return strings.Replace(source, old, newChanPrefix, -1)
}

func (c *ComponentShow) CollectionCommand(ctx context.Context, p *CollectionParam) (*Collections, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
	defer cancel()

	count := 0
	fmt.Println("== start to rename remained keys")
	prefix := "in01-6d348b125a73b89/kv"
	err := c.client.WalkWithPrefix(ctx, prefix, 10, func(key []byte, value []byte) error {
		k := string(key)
		newKey := replace(k, "in01-6d348b125a73b89", "6d348b125a73b89")
		err := c.client.Save(ctx, newKey, string(value))
		if err != nil {
			fmt.Printf("rename key:%s, update value fails\n", key)
			return err
		}

		err = c.client.Remove(ctx, k)
		if err != nil {
			fmt.Printf("delete key:%s fails\n", key)
			return err
		}
		fmt.Printf("rename key:%s to %s done\n", key, newKey)

		count++
		return nil
	})

	if err != nil {
		fmt.Printf("rename fails, %w\n", err)
	}
	return &Collections{}, nil
}

type Collections struct {
	collections []*models.Collection
	total       int64
	channels    int
	healthy     int
}

func (rs *Collections) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		for _, coll := range rs.collections {
			printCollection(sb, coll)
		}
		fmt.Fprintln(sb, "================================================================================")
		fmt.Printf("--- Total collections:  %d\t Matched collections:  %d\n", rs.total, len(rs.collections))
		fmt.Printf("--- Total channel: %d\t Healthy collections: %d\n", rs.channels, rs.healthy)
		return sb.String()
	}
	return ""
}

func (rs *Collections) Entities() any {
	return rs.collections
}

func printCollection(sb *strings.Builder, collection *models.Collection) {
	fmt.Println("================================================================================")
	fmt.Printf("DBID: %d\n", collection.DBID)
	fmt.Printf("Collection ID: %d\tCollection Name: %s\n", collection.ID, collection.Schema.Name)
	t, _ := utils.ParseTS(collection.CreateTime)
	fmt.Printf("Collection State: %s\tCreate Time: %s\n", collection.State.String(), t.Format("2006-01-02 15:04:05"))
	/*
		fmt.Printf("Partitions:\n")
		for idx, partID := range collection.GetPartitionIDs() {
			fmt.Printf(" - Partition ID: %d\tPartition Name: %s\n", partID, collection.GetPartitionNames()[idx])
		}*/
	fmt.Printf("Fields:\n")
	fields := collection.Schema.Fields
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].FieldID < fields[j].FieldID
	})
	for _, field := range fields {
		fmt.Printf(" - Field ID: %d \t Field Name: %s \t Field Type: %s\n", field.FieldID, field.Name, field.DataType.String())
		if field.IsPrimaryKey {
			fmt.Printf("\t - Primary Key: %t, AutoID: %t\n", field.IsPrimaryKey, field.AutoID)
		}
		if field.IsDynamic {
			fmt.Printf("\t - Dynamic Field\n")
		}
		if field.IsPartitionKey {
			fmt.Printf("\t - Partition Key\n")
		}
		for key, value := range field.Properties {
			fmt.Printf("\t - Type Param %s: %s\n", key, value)
		}
	}

	fmt.Printf("Enable Dynamic Schema: %t\n", collection.Schema.EnableDynamicSchema)
	fmt.Printf("Consistency Level: %s\n", collection.ConsistencyLevel.String())
	for _, channel := range collection.Channels {
		fmt.Printf("Start position for channel %s(%s): %v\n", channel.PhysicalName, channel.VirtualName, channel.StartPosition.MsgID)
	}
}
