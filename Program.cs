using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TestKafkaPublishService
{
    class Program
    {
        static void Main(string[] args)
        {
            var provider = new TestKafkaProvider(CancellationToken.None, "10.79.84.60:9092,10.79.85.103:9092,10.79.85.104:9092", "ESPN.ESD.AdVisor.FakeTest");
            for (int i = 1; i <= 10; i++)
            {
                provider.Publish($"Say Hello {i}", "ESPN.ESD.ADVISOR.TEST.MESSAGE.UPDATE");
            }
        }
    }
}
