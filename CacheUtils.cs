using Apache.Ignite.Core;
using Apache.Ignite.Core.Binary;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Cache.Query;
using System;
using System.Collections;
using testIgnite.CacheStores;

namespace testIgnite
{
    class CacheUtils
    {
        private IIgnite ignite;
        private ICache<TestObjectPK, TestObject> cacheTestObject;


        public void startCache()
        {
            IgniteConfiguration config = new IgniteConfiguration
            {
                //register classes 
                BinaryConfiguration = new BinaryConfiguration(typeof(TestObject), typeof(TestObjectPK))
            };


            try
            {
                ignite = Ignition.Start(config);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }

            //create cache for TestObject
            cacheTestObject = ignite.GetOrCreateCache<TestObjectPK, TestObject>(new CacheConfiguration
            {

                Name = "TestObject",
                CacheStoreFactory = new OracleStoreFactory(),
                CacheMode = CacheMode.Replicated,
                ReadThrough = false,
                WriteThrough = true,
                KeepBinaryInStore = false,// Store works with concrete classes.

                QueryEntities = new[]
                {
                    new QueryEntity//NECESSARY TO MAKE POSSIBLE SQL QUERY
                    {
                        KeyType = typeof(TestObjectPK),
                        ValueType = typeof(TestObject),
                    }
                }

            });

            //Load cache
            Console.WriteLine("=====================================================");
            Console.WriteLine("Cache size before load:" + cacheTestObject.GetSize());
            cacheTestObject.LoadCache(null, null);
            Console.WriteLine("Cache size after load:" + cacheTestObject.GetSize());

            var fieldsQuery = new SqlFieldsQuery("select _val,_key from TestObject where NAME=? and VALUE=?", "h1", "val1");
            IQueryCursor<IList> queryCursor = cacheTestObject.QueryFields(fieldsQuery);
            Console.WriteLine("Query results:");
            foreach (IList fieldList in queryCursor)
                Console.WriteLine(" "+((TestObject)fieldList[0]).VALUE);

            Console.WriteLine("==============FINISH==============");
        }
    }
}
