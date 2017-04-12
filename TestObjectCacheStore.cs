using Apache.Ignite.Core.Cache.Store;
using Apache.Ignite.Core.Common;
using Oracle.DataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;

namespace testIgnite.CacheStores
{
    public class OracleStore : CacheStoreAdapter
    {
        public readonly static string info = "Data Source=ip/orcl12c;User Id=user;Password=pass;Pooling=true;Statement Cache Size=20;Enlist=false;";

        public override void LoadCache(Action<object, object> act, params object[] args)
        {
            string sql = "SELECT * FROM TESTOBJECT";
            using (var con = new OracleConnection
            {
                ConnectionString = info
            })
            {
                con.Open();
                OracleCommand oraCmd = new OracleCommand();
                oraCmd.Connection = con;
                oraCmd.CommandText = sql;
                oraCmd.CommandType = CommandType.Text;
                using (OracleDataReader dataReader = oraCmd.ExecuteReader())
                {
                    if (dataReader != null)
                    {
                        if (dataReader.HasRows)
                        {
                            while (dataReader.Read())
                            {

                                TestObjectPK pk = new TestObjectPK();
                                TestObject obj = new TestObject();

                                for (int i = 0; i < dataReader.FieldCount; i++)
                                {
                                    String columnName = dataReader.GetName(i);
                                    if (columnName.Equals("ID"))
                                    {
                                        pk.ID = dataReader.GetInt32(i);
                                    }
                                    else if (columnName.Equals("NAME"))
                                    {
                                        pk.NAME = dataReader.GetString(i);
                                    }
                                    else if (columnName.Equals("VALUE"))
                                    {
                                        obj.VALUE = dataReader.GetString(i);
                                    }
                                }
                                act(pk, obj);
                            }
                        }
                    }
                }
            }
        }

        public override object Load(object key)
        {
            using (var con = new OracleConnection
            {
                ConnectionString = info
            })
            {
                con.Open();
                var cmd = con.CreateCommand();
                cmd.CommandText = "SELECT * FROM TESTOBJECT WHERE NAME=:name";
                cmd.BindByName = true;
                cmd.Parameters.Add("name", OracleDbType.Varchar2, (String)key, ParameterDirection.Input);
                using (OracleDataReader dataReader = cmd.ExecuteReader())
                {
                    dataReader.Read();
                    // Read data, return as object
                    TestObject obj = new TestObject();
                    for (int i = 0; i < dataReader.FieldCount; i++)
                    {
                        String columnName = dataReader.GetName(i);
                        if (columnName.Equals("VALUE"))
                        {
                            obj.VALUE = dataReader.GetString(i);
                        }
                    }
                    return obj;
                }
            }
        }


        public override void Write(object key, object val)
        {
            using (var conn = new OracleConnection
            {
                ConnectionString = info
            })
            {
                conn.Open();
                OracleTransaction txn = conn.BeginTransaction();
                OracleCommand cmd = new OracleCommand();
                // Set the command text on an OracleCommand object
                cmd.CommandText = @"BEGIN
              INSERT INTO TESTOBJECT(ID, NAME, VALUE) VALUES(:key,:name, :val );
                        EXCEPTION
                          WHEN DUP_VAL_ON_INDEX THEN
                UPDATE TESTOBJECT
                SET VALUE = :val
                WHERE ID = :key; and NAME=:name
                        END; ";
                cmd.Connection = conn;
                cmd.BindByName = true;  ///////because i will name the paramaters
                cmd.Parameters.Add("key", OracleDbType.Int32, ((TestObjectPK)key).ID, ParameterDirection.Input);
                cmd.Parameters.Add("name", OracleDbType.NVarchar2, ((TestObjectPK)key).NAME, ParameterDirection.Input);
                cmd.Parameters.Add("val", OracleDbType.NVarchar2, ((TestObject)val).VALUE, ParameterDirection.Input);

                // Execute the command
                cmd.ExecuteNonQuery();
                txn.Commit();

                txn.Dispose();
                foreach (OracleParameter p in cmd.Parameters)
                {
                    p.Dispose();
                }
                cmd.Dispose();
            }

        }

        public override void Delete(object key)
        {
            using (var conn = new OracleConnection
            {
                ConnectionString = info
            })
            {
                conn.Open();
                OracleTransaction txn = conn.BeginTransaction();
                OracleCommand cmd = new OracleCommand();
                // Set the command text on an OracleCommand object
                cmd.CommandText = @"delete from TESTOBJECT Where ID=:key and  NAME=:name";
                cmd.Connection = conn;
                cmd.BindByName = true;  ///////because i will name the paramaters
                cmd.Parameters.Add("key", OracleDbType.Int32, ((TestObjectPK)key).ID, ParameterDirection.Input);
                cmd.Parameters.Add("name", OracleDbType.Int32, ((TestObjectPK)key).NAME, ParameterDirection.Input);
                // Execute the command
                cmd.ExecuteNonQuery();
                txn.Commit();

                txn.Dispose();
                foreach (OracleParameter p in cmd.Parameters)
                {
                    p.Dispose();
                }
                cmd.Dispose();
            }
        }

    }

    [Serializable]
    public class OracleStoreFactory : IFactory<OracleStore>
    {
        public OracleStore CreateInstance()
        {
            return new OracleStore();
        }
    }
}
