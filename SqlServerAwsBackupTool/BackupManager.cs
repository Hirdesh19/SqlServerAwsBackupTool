using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using IniParser;
using IniParser.Model;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlServerAwsBackupTool
{
    public class BackupManager
    {
        public BackupResult Backup(string[] args)
        {
            if (args.Length < 1)
            {
                Console.Error.WriteLine("You must specify a configuration file.");
                return new BackupResult(-1);
            }

            var configFileName = args[0];

            if (!File.Exists(configFileName))
            {
                Console.Error.WriteLine("You must specify a configuration file that exists.");
                return new BackupResult(-2);
            }

            var parser = new FileIniDataParser();
            IniData config = parser.ReadFile(configFileName);

            // SqlServer variables
            var serverName = config["sqlserver"]["server"];
            var database = config["sqlserver"]["database"];
            var tmpFolder = config["sqlserver"]["temp_dir"];

            // Aws variables
            var retentionPolicy = int.Parse(config["general"]["retention_policy"]) * -1;

            // Use the date to create a unique name for the backup
            var dateString = DateTime.UtcNow.ToString("yyyy_MM_dd_HH_mm_ss");
            var bkpName = serverName + "_" + database + "_" + dateString + ".bak";
            var bkpPath = Path.Combine(tmpFolder, bkpName);

            // Perform the backup
            if (!Directory.Exists(tmpFolder))
            {
                Directory.CreateDirectory(tmpFolder);
            }

            var serverConn = new ServerConnection(serverName);
            var server = new Server(serverConn);
            var backup = new Backup();

            backup.Action = BackupActionType.Database;
            backup.Database = database;
            backup.Devices.AddDevice(bkpPath, DeviceType.File);

            backup.SqlBackup(server);

            serverConn.Disconnect();

            // Put the object on S3
            var uploadReq = new PutObjectRequest()
            {
                BucketName = config["aws"]["bucket"],
                Key = bkpName,
                FilePath = bkpPath
            };

            var awsProfile = "sql_server_backup";

            Amazon.Util.ProfileManager.RegisterProfile(awsProfile, config["aws"]["access_key"], config["aws"]["secret_key"]);

            var creds = Amazon.Util.ProfileManager.GetAWSCredentials(awsProfile);

            var awsClient = new AmazonS3Client(creds, Amazon.RegionEndpoint.USEast1);

            awsClient.PutObject(uploadReq);

            // Remove the backup from the local system
            File.Delete(bkpPath);

            // Cleanup the S3 bucket based on the retention policy
            var twoWeeksAgo = DateTime.Now.AddMinutes(retentionPolicy);
            var listReq = new ListObjectsRequest()
            {
                BucketName = config["aws"]["bucket"]
            };

            var objects = awsClient.ListObjects(listReq);

            foreach (var obj in objects.S3Objects)
            {
                if (obj.LastModified < twoWeeksAgo)
                {
                    var delReq = new DeleteObjectRequest() { BucketName = config["aws"]["bucket"], Key = obj.Key };

                    awsClient.DeleteObject(delReq);
                }
            }

            return new BackupResult(bkpName);
        }
    }
}
