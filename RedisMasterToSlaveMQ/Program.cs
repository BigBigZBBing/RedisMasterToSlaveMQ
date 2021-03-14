using NewLife.Caching;
using Newtonsoft.Json;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace RedisMasterToSlaveMQ
{
    public struct Packet
    {
        public string UniqueNo;
        public string Message;
        public string CreateTime;
    }

    class Program
    {
        static RedisReliableQueue<string> ListContainer;
        static RedisReliableQueue<string> SlaveListContainer;
        static FullRedis master, slave1;
        static int maxContainerNumber = 100;
        static string ListKey = "testList";
        static string[] ListKeys = new[] { ListKey + 1, ListKey + 2 };

        static void Main(string[] args)
        {
            master = new FullRedis("127.0.0.1:6379", "zbb8484284", 0);
            slave1 = new FullRedis("127.0.0.1:6378", "zbb8484284", 0);
            master.Clear();

            //一个集合 保存所有的List容器
            master.SADD(ListKey, ListKeys);
            //容器的阀门
            for (int i = 0; i < ListKeys.Length; i++)
            {
                bool start = false;
                if (i == 0) start = true;
                master.Set(ListKeys[i], start);
            }

            IdWorker idworker = new IdWorker(1);
            Task.Factory.StartNew(() =>
            {
                while (true)
                {
                    AssignProducer();
                    Packet packet;
                    packet.UniqueNo = idworker.nextId().ToString();
                    packet.Message = GetRandomString(100, true, true, true, true, "");
                    packet.CreateTime = DateTime.Now.ToString();
                    ListContainer.Add(JsonConvert.SerializeObject(packet));
                    Console.WriteLine($"队列名:{ListContainer.Key}[]生产:{packet.Message}");
                    Thread.Sleep(100);
                }
            });

            while (true)
            {
                bool taskError = true;
                try
                {
                    AssignConsumer();
                    if (SlaveListContainer != null && SlaveListContainer.Count > 0)
                    {
                        var message = SlaveListContainer.Take().FirstOrDefault();
                        taskError = false;
                        Console.WriteLine($"队列名:{SlaveListContainer.Key}[]消费:{message}");
                        SlaveListContainer.Acknowledge(message);
                    }
                }
                catch (System.Exception ex)
                {
                    if (!taskError)
                        Console.WriteLine($"错误:{ex.Message}");
                }
            }
            slave1?.Dispose();
            master?.Dispose();
        }

        static object _lock = new object();
        static void AssignProducer()
        {
            var set = master.GetSet<string>("testList");
            if (ListContainer == null)
            {
                lock (_lock)
                {
                    if (ListContainer != null) return;
                    foreach (var item in set)
                    {
                        if (master.Get<bool>(item))
                            ListContainer = master.GetReliableQueue<string>(item + "BQueue");
                    }
                }
            }
            else if (ListContainer.Count >= maxContainerNumber)
            {
                lock (_lock)
                {
                    if (ListContainer.Count < maxContainerNumber) return;
                    foreach (var item in set)
                    {
                        if (item != ListContainer.Key.Replace("BQueue", ""))
                        {
                            master.Set(ListContainer.Key.Replace("BQueue", ""), false);
                            ListContainer = master.GetReliableQueue<string>(item + "BQueue");
                            master.Execute(item + "BQueue", client => client.Execute<string>("LTRIM", item + "BQueue", "1", "0"));
                            master.Set(item, true);
                            return;
                        }
                    }
                }
            }
        }

        static object _slavelock = new object();
        static string currentKey;
        static void AssignConsumer()
        {
            if (SlaveListContainer == null)
            {
                lock (_slavelock)
                {
                    if (SlaveListContainer != null) return;
                    var set = slave1.GetSet<string>(ListKey);
                    foreach (var item in set)
                    {
                        if (slave1.Get<bool>(item))
                        {
                            currentKey = item;
                            SlaveListContainer = slave1.GetReliableQueue<string>(item + "BQueue");
                            return;
                        }
                    }
                }
            }
            else if (!slave1.Get<bool>(currentKey) && SlaveListContainer.Count == 0)
            {
                lock (_slavelock)
                {
                    if (slave1.Get<bool>(currentKey)) return;
                    var set = slave1.GetSet<string>(ListKey);
                    foreach (var item in set)
                    {
                        if (slave1.Get<bool>(item))
                        {
                            currentKey = item;
                            SlaveListContainer = slave1.GetReliableQueue<string>(item + "BQueue");
                            return;
                        }
                    }
                }
            }
        }


        static string GetRandomString(int length, bool useNum, bool useLow, bool useUpp, bool useSpe, string custom)
        {
            byte[] b = new byte[4];
            new System.Security.Cryptography.RNGCryptoServiceProvider().GetBytes(b);
            Random r = new Random(BitConverter.ToInt32(b, 0));
            string s = null, str = custom;
            if (useNum == true) { str += "0123456789"; }
            if (useLow == true) { str += "abcdefghijklmnopqrstuvwxyz"; }
            if (useUpp == true) { str += "ABCDEFGHIJKLMNOPQRSTUVWXYZ"; }
            if (useSpe == true) { str += "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"; }
            for (int i = 0; i < length; i++)
            {
                s += str.Substring(r.Next(0, str.Length - 1), 1);
            }
            return s;
        }
    }

    public class IdWorker
    {
        //机器ID
        private static long workerId;
        private static long twepoch = 687888001020L; //唯一时间，这是一个避免重复的随机量，自行设定不要大于当前时间戳
        private static long sequence = 0L;
        private static int workerIdBits = 4; //机器码字节数。4个字节用来保存机器码(定义为Long类型会出现，最大偏移64位，所以左移64位没有意义)
        public static long maxWorkerId = -1L ^ -1L << workerIdBits; //最大机器ID
        private static int sequenceBits = 10; //计数器字节数，10个字节用来保存计数码
        private static int workerIdShift = sequenceBits; //机器码数据左移位数，就是后面计数器占用的位数
        private static int timestampLeftShift = sequenceBits + workerIdBits; //时间戳左移动位数就是机器码和计数器总字节数
        public static long sequenceMask = -1L ^ -1L << sequenceBits; //一微秒内可以产生计数，如果达到该值则等到下一微妙在进行生成
        private long lastTimestamp = -1L;

        /// <summary>
        /// 机器码
        /// </summary>
        /// <param name="workerId"></param>
        public IdWorker(long workerId)
        {
            if (workerId > maxWorkerId || workerId < 0)
                throw new Exception(string.Format("worker Id can't be greater than {0} or less than 0 ", workerId));
            IdWorker.workerId = workerId;
        }

        public long nextId()
        {
            lock (this)
            {
                long timestamp = timeGen();
                if (this.lastTimestamp == timestamp)
                { //同一微妙中生成ID
                    IdWorker.sequence = (IdWorker.sequence + 1) & IdWorker.sequenceMask; //用&运算计算该微秒内产生的计数是否已经到达上限
                    if (IdWorker.sequence == 0)
                    {
                        //一微妙内产生的ID计数已达上限，等待下一微妙
                        timestamp = tillNextMillis(this.lastTimestamp);
                    }
                }
                else
                { //不同微秒生成ID
                    IdWorker.sequence = 0; //计数清0
                }
                if (timestamp < lastTimestamp)
                { //如果当前时间戳比上一次生成ID时时间戳还小，抛出异常，因为不能保证现在生成的ID之前没有生成过
                    throw new Exception(string.Format("Clock moved backwards.  Refusing to generate id for {0} milliseconds",
                        this.lastTimestamp - timestamp));
                }
                this.lastTimestamp = timestamp; //把当前时间戳保存为最后生成ID的时间戳
                long nextId = (timestamp - twepoch << timestampLeftShift) | IdWorker.workerId << IdWorker.workerIdShift | IdWorker.sequence;
                return nextId;
            }
        }

        /// <summary>
        /// 获取下一微秒时间戳
        /// </summary>
        /// <param name="lastTimestamp"></param>
        /// <returns></returns>
        private long tillNextMillis(long lastTimestamp)
        {
            long timestamp = timeGen();
            while (timestamp <= lastTimestamp)
            {
                timestamp = timeGen();
            }
            return timestamp;
        }

        /// <summary>
        /// 生成当前时间戳
        /// </summary>
        /// <returns></returns>
        private long timeGen()
        {
            return (long)(DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
        }

    }
}
