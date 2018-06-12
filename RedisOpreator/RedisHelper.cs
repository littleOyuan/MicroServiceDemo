using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using StackExchange.Redis;

namespace RedisOpreator
{
    public class RedisHelper
    {
        private static readonly object Locker = new object();
        private static IConnectionMultiplexer _connMultiplexer;

        private static readonly string RedisConnectionString = string.Format("{0}:{1},password={2}", "10.10.21.214", "6379", "colipu123");

        private RedisHelper()
        {
        }

        private static IConnectionMultiplexer ConnectionMultiplexer
        {
            get
            {
                if (_connMultiplexer == null)
                {
                    lock (Locker)
                    {
                        if (_connMultiplexer == null)
                            _connMultiplexer = StackExchange.Redis.ConnectionMultiplexer.Connect(RedisConnectionString);
                    }
                }

                RegisterEvent();

                return _connMultiplexer;
            }
        }

        /// <summary>
        /// 获取Redis数据库
        /// </summary>
        /// <param name="db"></param>
        public static IDatabase GetDatabase(int db = -1)
        {
            return ConnectionMultiplexer.GetDatabase(db);
        }

        public static void KeyEventNotification(Action<string> action)
        {
            /*
                K：keyspace事件，事件以__keyspace@<db>__为前缀进行发布
                E：keyevent事件，事件以__keyevent@<db>__为前缀进行发布
                g：一般性的，非特定类型的命令，比如del，expire，rename等
                $：字符串特定命令
                l：列表特定命令
                s：集合特定命令
                h：哈希特定命令
                z：有序集合特定命令
                x：过期事件，当某个键过期并删除时会产生该事件
                e：驱逐事件，当某个键因maxmemore策略而被删除时，产生该事件
                A：g$lshzxe的别名，因此”AKE”意味着所有事件
             */

            IDatabase db = GetDatabase();

            //对应Redis执行 config set notify-keyspace-events Eg
            string notificationChannel = "__keyevent@" + db.Database + "__:del";

            ConnectionMultiplexer.GetSubscriber().Subscribe(notificationChannel, (channel, value) => { action(value); });

            /* 
            string notificationChannel = "__keyspace@" + db.Database + "__:*";
            ConnectionMultiplexer.GetSubscriber().Subscribe(notificationChannel, (channel, notificationType) =>
            {
                string typeValue = channel.ToString();

                var index = typeValue.IndexOf(':');
                if (index >= 0 && index < typeValue.Length - 1)
                    action(typeValue.Substring(index + 1));
            });
            */
        }

        public static bool KeyExists(string redisKey)
        {
            return GetDatabase().KeyExists(redisKey);
        }

        public static bool KeyRename(string redisKey, string redisNewKey)
        {
            return GetDatabase().KeyRename(redisKey, redisNewKey);
        }

        public static bool KeyExpire(string redisKey, TimeSpan? expiry)
        {
            return GetDatabase().KeyExpire(redisKey, expiry);
        }

        public static bool KeyDelete(string redisKey)
        {
            return GetDatabase().KeyDelete(redisKey);
        }

        public static long KeyDelete(IEnumerable<string> redisKeys)
        {
            return GetDatabase().KeyDelete(redisKeys.Select(key => (RedisKey)key).ToArray());
        }

        public static bool StringSet(string redisKey, string redisValue, TimeSpan? expiry = null)
        {
            return GetDatabase().StringSet(redisKey, redisValue, expiry);
        }

        public static bool StringSet<T>(string redisKey, T redisValue, TimeSpan? expiry = null)
        {
            return GetDatabase().StringSet(redisKey, Serialize(redisValue), expiry);
        }

        public static bool StringSet(IEnumerable<KeyValuePair<RedisKey, RedisValue>> keyValuePairs)
        {
            keyValuePairs = keyValuePairs.Select(kv => new KeyValuePair<RedisKey, RedisValue>(kv.Key, kv.Value));

            return GetDatabase().StringSet(keyValuePairs.ToArray());
        }

        public static string StringGet(string redisKey)
        {
            return GetDatabase().StringGet(redisKey);
        }

        public static T StringGet<T>(string redisKey)
        {
            return Deserialize<T>(GetDatabase().StringGet(redisKey));
        }

        public static bool SetAdd(string redisKey, string redisValue)
        {
            return GetDatabase().SetAdd(redisKey, redisValue);
        }

        public static long SetAdd(string redisKey, string[] redisValues)
        {
            return GetDatabase().SetAdd(redisKey, redisValues.Select(key => (RedisValue)key).ToArray());
        }

        public static bool SetRemove(string redisKey, string redisValue)
        {
            return GetDatabase().SetRemove(redisKey, redisValue);
        }

        public static long SetRemove(string redisKey, string[] redisValues)
        {
            return GetDatabase().SetRemove(redisKey, redisValues.Select(key => (RedisValue)key).ToArray());
        }

        #region 序列化、反序列化
        private static byte[] Serialize(object obj)
        {
            if (obj == null)
                return null;

            BinaryFormatter binaryFormatter = new BinaryFormatter();

            using (MemoryStream memoryStream = new MemoryStream())
            {
                binaryFormatter.Serialize(memoryStream, obj);

                byte[] objectDataAsStream = memoryStream.ToArray();

                return objectDataAsStream;
            }
        }

        private static T Deserialize<T>(byte[] stream)
        {
            if (stream == null)
                return default(T);

            BinaryFormatter binaryFormatter = new BinaryFormatter();

            using (MemoryStream memoryStream = new MemoryStream(stream))
            {
                T result = (T)binaryFormatter.Deserialize(memoryStream);

                return result;
            }
        }
        #endregion

        #region 注册事件
        private static void RegisterEvent()
        {
            _connMultiplexer.ConnectionRestored += ConnMultiplexer_ConnectionRestored;
            _connMultiplexer.ConnectionFailed += ConnMultiplexer_ConnectionFailed;
            _connMultiplexer.ErrorMessage += ConnMultiplexer_ErrorMessage;
            _connMultiplexer.ConfigurationChanged += ConnMultiplexer_ConfigurationChanged;
            _connMultiplexer.HashSlotMoved += ConnMultiplexer_HashSlotMoved;
            _connMultiplexer.InternalError += ConnMultiplexer_InternalError;
            _connMultiplexer.ConfigurationChangedBroadcast += ConnMultiplexer_ConfigurationChangedBroadcast;
        }

        /// <summary>
        /// 建立物理连接时
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void ConnMultiplexer_ConnectionRestored(object sender, ConnectionFailedEventArgs e)
        {
            //LogRecorder.Record(string.Format("{0}:{1}", "ConnMultiplexer_ConnectionRestored", e.Exception));
        }

        /// <summary>
        /// 物理连接失败时
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void ConnMultiplexer_ConnectionFailed(object sender, ConnectionFailedEventArgs e)
        {
            //LogRecorder.Record(string.Format("{0}:{1}", "ConnMultiplexer_ConnectionFailed", e.Exception));
        }

        /// <summary>
        /// 发生错误时
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void ConnMultiplexer_ErrorMessage(object sender, RedisErrorEventArgs e)
        {
            //LogRecorder.Record(string.Format("{0}:{1}", "ConnMultiplexer_ErrorMessage", e.Message));
        }

        /// <summary>
        /// 配置更改时
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void ConnMultiplexer_ConfigurationChanged(object sender, EndPointEventArgs e)
        {
            //LogRecorder.Record(string.Format("{0}:{1}", "ConnMultiplexer_ConfigurationChanged", e.EndPoint));
        }

        /// <summary>
        /// 更改集群时
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void ConnMultiplexer_HashSlotMoved(object sender, HashSlotMovedEventArgs e)
        {
            //LogRecorder.Record(string.Format("{0}:{1}-{2} To {3}-{4}", "ConnMultiplexer_HashSlotMoved", "e.OldEndPoint", e.OldEndPoint, "e.NewEndPoint", e.NewEndPoint));
        }

        /// <summary>
        /// 发生内部错误时（主要用于调试）
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void ConnMultiplexer_InternalError(object sender, InternalErrorEventArgs e)
        {
            //LogRecorder.Record(string.Format("{0}:{1}", "ConnMultiplexer_InternalError", e.Exception));
        }

        /// <summary>
        /// 重新配置广播时（通常意味着主从同步更改）
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void ConnMultiplexer_ConfigurationChangedBroadcast(object sender, EndPointEventArgs e)
        {
            //LogRecorder.Record(string.Format("{0}:{1}", "ConnMultiplexer_ConfigurationChangedBroadcast", e.EndPoint));
        }
        #endregion
    }
}
