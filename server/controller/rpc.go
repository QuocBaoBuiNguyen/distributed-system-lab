package controller

import (
	"encoding/json"
	"lab01_rpc/common"
	"log"
	"strconv"

	"github.com/marcelloh/fastdb"
)

type FastDBService struct {
	Repository *fastdb.DB
}

func (p *FastDBService) Set(args *common.SetArgs, reply *string) error {
	log.Printf("SET: Bucket=%s, Key=%d, Value=%s, Status=processing", args.Bucket, args.Key, args.Value)

	dbRecord, err := json.Marshal(args.Value)
	if err != nil {
		*reply = "Error while marshalling request"
		log.Printf("SET: Bucket=%s, Key=%d, Value=%s, Status=failure, Error=%v", args.Bucket, args.Key, args.Value, err)
		return err
	}

	p.Repository.Set(args.Bucket, args.Key, dbRecord)
	log.Printf("SET: Bucket=%s, Key=%d, Value=%s, Status=success", args.Bucket, args.Key, args.Value)
	*reply = "Saved successfully"
	return nil
}

func (p *FastDBService) Get(args *common.GetArgs, reply *string) error {
	log.Printf("GET: Bucket=%s, Key=%d, Status=processing", args.Bucket, args.Key)

	dbRecord, status := p.Repository.Get(args.Bucket, args.Key)
	if !status {
		*reply = "Key not found"
		log.Printf("GET: Bucket=%s, Key=%d, Status=not_found", args.Bucket, args.Key)
		return nil
	}

	*reply = string(dbRecord)
	log.Printf("GET: Bucket=%s, Key=%d, Status=success, Value=%s", args.Bucket, args.Key, *reply)
	return nil
}

func (p *FastDBService) GetAll(args *common.GetAllArgs, reply *map[int]string) error {
	log.Printf("GET ALL: Bucket=%s, Status=processing", args.Bucket)

	dbRecords, err := p.Repository.GetAll(args.Bucket)
	if err != nil {
		log.Printf("GET ALL: Bucket=%s, Status=failure, Error=%v", args.Bucket, err)
		return err
	}

	if len(dbRecords) == 0 {
		log.Printf("GET ALL: Bucket=%s, Status=not_found", args.Bucket)
		return nil
	}

	result := make(map[int]string)
	for key, record := range dbRecords {
		result[key] = string(record)
	}

	*reply = result
	log.Printf("GET ALL: Bucket=%s, Status=success, RecordCount=%d", args.Bucket, len(dbRecords))
	return nil
}

func (p *FastDBService) Delete(args *common.DeleteArgs, reply *string) error {
	log.Printf("DELETE: Bucket=%s, Key=%d, Status=processing", args.Bucket, args.Key)

	isDeleted, err := p.Repository.Del(args.Bucket, args.Key)

	if err != nil {
		log.Printf("DELETE: Bucket=%s, Key=%d, Status=failure, Error=%v", args.Bucket, args.Key, err)
		return err
	}

	if isDeleted {
		*reply = "Deleted entry (which owns key: " + strconv.Itoa(args.Key) + ") successfully."
		log.Printf("DELETE: Bucket=%s, Key=%d, Status=success", args.Bucket, args.Key)
		return nil
	}

	*reply = "Key not found."
	log.Printf("DELETE: Bucket=%s, Key=%d, Status=not_found", args.Bucket, args.Key)
	return nil
}
