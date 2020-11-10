# Go MySQL utils

##### Two main functions: database operation and automatic ORM(object(struct) relational mapping).

---------------------------------------

### Installation

```
$ go get -u github.com/Dzeqkon/goutils
$ go get -u github.com/Dzeqkon/mysql-orm
```

### Usage

##### 1. Create a new mysql client:

```
func TestDbClient() *DBClient {
	var dbConfig dzgmysql.DBConfig
	dbConfig.DbHost = "127.0.0.1"
	dbConfig.DbUser = "root"
	dbConfig.DbPass = "123456"
	dbConfig.IsLocalTime = true
	dbConfig.DbName = "test"
	return dzgmysql.NewDbClient(dbConfig)
}
```

##### 2. Create Object(struct) Relational Mapping:

```
func TestGenerateORM(t *testing.T) {
	client := TestDbClient()
	orm := dzgmysql.NewORMGenerator(client)
	orm.AddComment = true
	tabNames := []string{"we_test_tab1", "we_test_tab2"}
	orm.DefaultGenerators(tabNames)
	client.CloseConn()
}
```

