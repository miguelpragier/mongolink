package mongolink

import (
	"context"
	"errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"os"
	"time"
)


// mongodb+srv://ghostbaker:D1G1T4LUND3RGR0UND@bakery01-ordh5.gcp.mongodb.net/test?retryWrites=true

// Connection is the concentrator for all mongodb cluster links to different databases
type Connection struct {
	link *mongo.Client
}

func (c*Connection) connect() error {
	// Configurations can be empty if something weird happened OR it's the first use.
	// If it so, reload directives
	connectionString:=os.Getenv("mongolink")

	if connectionString=="" {
		panic(errors.New("there's no connection string for mongodb"))
	}

	wc:=writeconcern.New(writeconcern.J(false),writeconcern.WTimeout(5*time.Second))

	opts:=options.Client()
	opts.WriteConcern = wc

	client, err := mongo.NewClient(options.Client().ApplyURI(connectionString),opts)

	if err != nil {
		return err
	}

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	if err1:=client.Connect(ctx);err1 != nil {
		return err1
	}

	if err2:=client.Ping(ctx,readpref.Primary());err2 != nil {
		return err2
	}

	c.link=client

	return nil
}

// Collection returns a collection from the target database
// If database can't be connected, returns err!=nil
func (c*Connection) Collection(database string, collection string) (*mongo.Collection, error) {
	if c.link==nil{
		return nil,errors.New("use of uninitialized connection")
	}

	if err:=c.link.Ping(context.TODO(),nil);err != nil {
		if err:=c.connect();err!=nil{
			return nil,err
		}
	}

	return c.link.Database(database).Collection(collection),nil
}

// New returns a connected mongodb session
func New(panicIfFails bool) (*Connection, error) {
	var c Connection

	if err:=c.connect();err!=nil{
		if panicIfFails {
			panic(err)
		}

		return nil,err
	}

	return &c,nil
}

// Close tries to disconnect from database
func(c*Connection)Close(){
	if c!=nil{
		if c.link!=nil{
			_=c.link.Disconnect(context.TODO())
		}
	}
}
