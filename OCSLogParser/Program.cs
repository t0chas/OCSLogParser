using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OCSLogParser
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 3)
                return;
            string dbName = args[0];
            string collectionName = args[1];
            string path = Path.GetFullPath(args[2]);

            LogParser parser = new LogParser(new Storage() { DatabaseName = dbName, CollectionName = collectionName });
            LogReader logReader = new LogReader(parser);
            var fileArr = System.IO.Directory.GetFiles(path, "*.log");
            List<string> files = new List<string>(fileArr);
            files.Sort();
            foreach(string f in files)
            {
                Console.WriteLine("Processing file '{0}'", f);
                parser.FileName = Path.GetFileName(f);
                logReader.Read(new StreamReader(f));
            }
            logReader.Finish();
            Console.WriteLine("Finished");
        }
    }
}
