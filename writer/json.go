package writer

import (
	"io"

	"github.com/xitongsys/parquet-go-source/writerfile"
	"github.com/xitongsys/parquet-go/marshal"
	"github.com/xitongsys/parquet-go/source"
)

type JSONWriter struct {
	ParquetWriter
}

func NewJSONWriterFromWriter(jsonSchema string, w io.Writer, np int64) (*ParquetWriter, error) {
	wf := writerfile.NewWriterFile(w)
	return NewJSONWriter(jsonSchema, wf, np)
}

func NewJSONWriter(jsonSchema string, pFile source.ParquetFile, np int64) (*ParquetWriter, error) {
	res, err := NewParquetWriter(pFile, jsonSchema, np)
	if err != nil {
		return nil, err
	}
	res.MarshalFunc = marshal.MarshalJSON
	return res, err
}

/*
//Create JSON writer
func NewJSONWriter(jsonSchema string, pfile source.ParquetFile, np int64) (*ParquetWriter, error) {
	var err error
	//res := new(JSONWriter)
	res := new(ParquetWriter)
	res.SchemaHandler, err = schema.NewSchemaHandlerFromJSON(jsonSchema)
	if err != nil {
		return res, err
	}

	res.PFile = pfile
	res.PageSize = 8 * 1024              //8K
	res.RowGroupSize = 128 * 1024 * 1024 //128M
	res.CompressionType = parquet.CompressionCodec_SNAPPY
	res.PagesMapBuf = make(map[string][]*layout.Page)
	res.DictRecs = make(map[string]*layout.DictRecType)
	res.NP = np
	res.Footer = parquet.NewFileMetaData()
	res.Footer.Version = 1
	res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)
	res.Offset = 4
	_, err = res.PFile.Write([]byte("PAR1"))
	res.MarshalFunc = marshal.MarshalJSON
	return res, err
}

*/
