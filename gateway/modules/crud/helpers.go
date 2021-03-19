package crud

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/spaceuptech/helpers"

	"github.com/spaceuptech/space-cloud/gateway/config"
)

func (m *Module) createBatch(ctx context.Context, project, dbAlias, col string, doc interface{}) (int64, error) {
	response := make(batchResponseChan, 1)
	defer close(response)

	var docsInserted int64
	var docArray []interface{}
	switch docType := doc.(type) {
	case map[string]interface{}:
		docsInserted = 1
		docArray = []interface{}{docType}
	case []interface{}:
		docsInserted = int64(len(docType))
		docArray = docType
	default:
		return 0, helpers.Logger.LogError(helpers.GetRequestID(ctx), fmt.Sprintf("Cannot create batch request unkownd doc type (%T) provided)", docType), nil, nil)
	}

	// Simply return if 0 docs are to be inserted
	if docsInserted == 0 {
		return 0, nil
	}

	ch, ok := m.batchMapTableToChan[project][dbAlias][col] // get channel for specified table
	if !ok {
		return 0, helpers.Logger.LogError(helpers.GetRequestID(ctx), "Cannot convert insert request to batch request", fmt.Errorf("cannot find channel for database %s & collection %s", dbAlias, col), nil)
	}
	ch.request <- batchRequest{documents: docArray, response: response}
	result := <-response
	return docsInserted, result.err
}

func getPreparedQueryKey(dbAlias, id string) string {
	return fmt.Sprintf("%s--%s", dbAlias, id)
}

// NOTE: the parent function should take lock on module before calling this function
func (m *Module) getDBInfo(dbAlias string) (*config.DatabaseConfig, error) {
	/*if m.alias != dbAlias {
		return nil, fmt.Errorf("dbAlias (%s) does not exists", dbAlias)
	}
	return m.config, nil*/

	i := 0
	for key, mm := range m.modules {
		fmt.Println(strconv.Itoa(i)+"+++"+key+":::getDBInfo===========project:", mm)
		i++
		fmt.Println(strconv.Itoa(i)+"+++"+key+":::getDBInfo===========alias:", m.alias+"====:"+dbAlias+"=====:"+mm.alias)
		fmt.Println(strconv.Itoa(i)+"+++"+key+":::getDBInfo===========dbType:", m.dbType+"=====:"+mm.dbType)
		fmt.Println(strconv.Itoa(i)+"+++"+key+":::getDBInfo===========project:", m.project+"=====:"+mm.project)

		if mm.config != nil && mm.alias == dbAlias {
			return mm.config, nil
		}
	}

	return nil, fmt.Errorf("dbAlias (%s) does not exists", dbAlias)
}

func (m *Module) getCrudBlock(dbAlias string) (Crud, error) {
	fmt.Println("getCrudBlock===========m.modules:", m.modules)
	for _, mm := range m.modules {
		fmt.Println("getCrudBlock===========alias:", m.alias+"====:"+dbAlias+"=====:"+mm.alias)
		fmt.Println("getCrudBlock===========dbType:", m.dbType+"=====:"+mm.dbType)
		fmt.Println("getCrudBlock===========project:", m.project+"=====:"+mm.project)

		if mm.block != nil && mm.alias == dbAlias {
			return mm.block, nil
		}
	}
	return nil, helpers.Logger.LogError(helpers.GetRequestID(context.TODO()), "Unable to get database connection, ensure you have added a database", fmt.Errorf("crud module not initialized for database (%s)", dbAlias), nil)
}

// splitConnectionString splits the connection string
func splitConnectionString(connection string) (string, bool) {
	s := strings.Split(connection, ".")
	if s[0] == "secrets" {
		return s[1], true
	}
	return "", false
}

func (m *Module) getDBType(dbAlias string) (string, error) {
	dbAlias = strings.TrimPrefix(dbAlias, "sql-")
	/*if dbAlias != m.alias {
		return "", fmt.Errorf("cannot get db type as invalid db alias (%s) provided", dbAlias)
	}
	return m.dbType, nil*/

	for _, mm := range m.modules {
		fmt.Println("getDBType===========alias:", m.alias+"====:"+dbAlias+"=====:"+mm.alias)
		fmt.Println("getDBType===========dbType:", m.dbType+"=====:"+mm.dbType)
		fmt.Println("getDBType===========project:", m.project+"=====:"+mm.project)

		if mm.alias == dbAlias {
			return mm.dbType, nil
		}
	}

	return "", fmt.Errorf("cannot get db type as invalid db alias (%s) provided", dbAlias)
}
