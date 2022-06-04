const ksqljs = require('@ksqljs/client');
require('dotenv').config();

const client = new ksqljs({
  ksqldbURL: 'http://0.0.0.0:8088'
});

//Confluent Clout Client
/* const client = new ksqljs({
  ksqldbURL: 'https://pksqlc-755v2.us-east-2.aws.confluent.cloud:443',
  API: process.env.KSQL_API_KEY,
  secret: process.env.KSQL_API_SECRET
}); */

const listStream = async () => {
  console.log(await client.ksql('LIST STREAMS;'));
};

const createStream = async () => {
  try {
    const data = await client.createStream(
      'userStream',
      ['name VARCHAR', 'email VARCHAR', 'age INTEGER'],
      'userTopic',
      'json',
      1
    );
  } catch (error) {
    console.log(error);
  };

};

const startPushQuery = async () => {
  try {
    await client.push(
      'SELECT * FROM userStream EMIT CHANGES;',
      (data) => { console.log(data) }
    );
  } catch (error) {
    console.log(error);
  };
};

const dropStream = async () => {
  try {
    await client.ksql('DROP TABLE IF EXISTS userAgeTable;');
    await client.ksql('DROP STREAM IF EXISTS userStream DELETE TOPIC;');
  } catch (error) {
    console.log(error);
  };
};

const insertIntoStream = async () => {
  try {
    const response = await client.insertStream('userStream', [
      { "name": "Matty X", "email": "123@mail.com", "age": 12 },
      { "name": "Micha S", "email": "123@mail.com", "age": 85 },
      { "name": "Jonat L", "email": "123@mail.com", "age": 98 },
      { "name": "Javan A", "email": "123@mail.com", "age": 40 },
      { "name": "Gerry B", "email": "123@mail.com", "age": 32 }
    ]);
  } catch (error) {
    console.log(error);
  }
};

const updateAge = async () => {
  const response = await client.insertStream('userStream', [
    { "name": "Matty X", "email": "123@mail.com", "age": 200 },
    { "name": "Micha S", "email": "123@mail.com", "age": 159 },
    { "name": "Jonat L", "email": "123@mail.com", "age": 109 },
    { "name": "Javan A", "email": "123@mail.com", "age": 231 },
    { "name": "Gerry B", "email": "123@mail.com", "age": 567 }
  ]);
};

const pullFromStream = async () => {
  try {
    const data = await client.pull('SELECT * FROM userStream;');
    console.log(data);
  } catch (error) {
    console.log(error);
  };
};

const realAgeTable = async () => {
  try {
    const data = await client.createTableAs(
      'userAgeTable',
      'userStream',
      ['name', 'LATEST_BY_OFFSET(age) AS recentAge'],
      { topic: 'userTopic' },
      {
        WHERE: 'age <= 120',
        GROUP_BY: 'name'
      }
    );
  } catch (error) {
    console.log(error);
  };
};

const pullFromMaterializedTable = async () => {
  try {
    const data = await client.pull('SELECT * FROM userAgeTable;');
    console.log(data);
  } catch (error) {
    console.log(error);
  };
}

// query builder demo
const queryBuilderTest = async () => {
  const finishedQuery = builder.build(query, 123, "middle' OR 1=1");
  console.log(finishedQuery)
  // the output query has the single quotes escaped so we don't terminate the string early
  // we also added feature to check for semi colons which are not desired as they end the query
  // "SELECT * FROM table WHERE id = 123 AND size = 'middle'' OR 1=1'"
}

// listStream();
// createStream();
// pullFromStream();
// insertIntoStream();
// startPushQuery();
// dropStream();
// realAgeTable();
// pullFromMaterializedTable();
// updateAge();