using IniParser;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using System.IO;
using System.Linq;
using IniParser.Model;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;

namespace SqlServerAwsBackupTool.Tests
{
    [TestFixture]
    public class BasicScenarioTests
    {
        private string tmpFolder = @"C:\temp\";
        private string defaultConfigIni = @"C:\SqlServerAwsBackupTool\config.ini";

        [TestFixtureSetUp]
        public void SetupTests()
        {
            if (!Directory.Exists(tmpFolder))
            {
                Directory.CreateDirectory(tmpFolder);
            }
        }

        [TestCase]
        public void BasicFullBackupScenario1()
        {
            var dbName = "basic_scenario_one";
            var backupFile = "";
            var testConfigFileName = tmpFolder + "BasicScenario1.ini";

            var parser = new FileIniDataParser();
            var config = parser.ReadFile(defaultConfigIni);

            config["sqlserver"].SetKeyData(new KeyData("database") { Value = dbName });

            parser.WriteFile(testConfigFileName, config);

            var conn = new SqlConnection(string.Format("Server={0};Database=master;Trusted_Connection=True;", config["sqlserver"]["server"]));

            conn.Open();

            try
            {
                // Create the database and insert some data
                conn.Execute("create database " + dbName);
                conn.ChangeDatabase(dbName);
                conn.Execute("create table test1 (test1 varchar(50))");
                conn.Execute("insert into test1 values ('data')");

                // Backup the database
                var backupManager = new BackupManager();
                var backupResult = backupManager.Backup(new string[2] { "full", testConfigFileName });

                Assert.AreEqual(0, backupResult.ReturnCode);
                Assert.IsFalse(File.Exists(backupFile));

                // Drop the database
                conn.ChangeDatabase("master");
                conn.Execute("drop database " + dbName);

                // Restore the backup from S3
                var restoreMan = new RestoreManager();

                restoreMan.Restore(new string[2] { testConfigFileName, backupResult.BackupName });

                var awsProfile = config["aws"]["profile"];

                Amazon.Util.ProfileManager.RegisterProfile(awsProfile, config["aws"]["access_key"], config["aws"]["secret_key"]);

                var creds = Amazon.Util.ProfileManager.GetAWSCredentials(awsProfile);

                var awsClient = new AmazonS3Client(creds, Amazon.RegionEndpoint.USEast1);

                conn.ChangeDatabase(dbName);
                var result = conn.Query("select test1 from test1 where test1 = 'data'");

                Assert.IsTrue(result.Count() == 1);

                var listReq = new ListObjectsRequest()
                {
                    BucketName = config["aws"]["bucket"]
                };

                var objects = awsClient.ListObjects(listReq);

                foreach (var obj in objects.S3Objects)
                {
                    if (obj.Key.IndexOf(dbName) != -1)
                    {
                        var delReq = new DeleteObjectRequest() { BucketName = config["aws"]["bucket"], Key = obj.Key };

                        awsClient.DeleteObject(delReq);
                    }
                }
            }
            finally
            {
                conn.ChangeDatabase("master");
                conn.Execute("drop database " + dbName);

                if (File.Exists(backupFile))
                {
                    File.Delete(backupFile);
                }
            }
        }

        [TestCase]
        public void BasicIncrementalBackupScenario2()
        {
            var dbName = "basic_scenario_two";
            var backupFile = "";
            var testConfigFileName = tmpFolder + "BasicScenario2.ini";

            var parser = new FileIniDataParser();
            var config = parser.ReadFile(defaultConfigIni);

            config["sqlserver"].SetKeyData(new KeyData("database") { Value = dbName });

            parser.WriteFile(testConfigFileName, config);

            var conn = new SqlConnection(string.Format("Server={0};Database=master;Trusted_Connection=True;", config["sqlserver"]["server"]));

            conn.Open();

            var logBackups = new List<string>();

            try
            {
                // Create the database and insert some data
                conn.Execute("create database " + dbName);
                conn.Execute("alter database " + dbName + " set recovery full");
                conn.ChangeDatabase(dbName);
                conn.Execute("create table test1 (test1 varchar(50))");
                conn.Execute("insert into test1 values ('data_full')");

                // Backup the database
                var backupManager = new BackupManager();
                var backupResult = backupManager.Backup(new string[2] { "full", testConfigFileName });

                Assert.AreEqual(0, backupResult.ReturnCode);
                Assert.IsFalse(File.Exists(backupResult.BackupName));

                // Do incremental stuff
                for (int i = 1; i <= 3; i++)
                {
                    // The minimum increment is a second between log backups
                    System.Threading.Thread.Sleep(1000);
                    conn.Execute(string.Format("insert into test1 values ('data_log_{0}')", i));

                    var backupLogResult = backupManager.Backup(new string[2] { "incremental", testConfigFileName });

                    Assert.AreEqual(0, backupLogResult.ReturnCode);
                    Assert.IsFalse(File.Exists(backupLogResult.BackupName));

                    logBackups.Add(backupLogResult.BackupName);
                }

                // Drop the database
                conn.ChangeDatabase("master");
                conn.Execute("drop database " + dbName);

                // Restore the backup using the restore manager
                var restoreMan = new RestoreManager();

                var argList = new List<string>() { testConfigFileName, backupResult.BackupName };

                argList.AddRange(logBackups);

                restoreMan.Restore(argList.ToArray());

                // Verify that the restore worked
                conn.ChangeDatabase(dbName);
                var result = conn.Query("select test1 from test1 where test1 in ('data_full', 'data_log_1', 'data_log_2', 'data_log_3')");

                Assert.IsTrue(result.Count() == 4);
            }
            finally
            {
                // Cleanup our mess
                // S3
                var awsProfile = config["aws"]["profile"];
                Amazon.Util.ProfileManager.RegisterProfile(awsProfile, config["aws"]["access_key"], config["aws"]["secret_key"]);
                var creds = Amazon.Util.ProfileManager.GetAWSCredentials(awsProfile);
                var awsClient = new AmazonS3Client(creds, Amazon.RegionEndpoint.USEast1);
                var listReq = new ListObjectsRequest()
                {
                    BucketName = config["aws"]["bucket"]
                };

                var objects = awsClient.ListObjects(listReq);

                foreach (var obj in objects.S3Objects)
                {
                    if (obj.Key.IndexOf(dbName) != -1)
                    {
                        var delReq = new DeleteObjectRequest() { BucketName = config["aws"]["bucket"], Key = obj.Key };

                        awsClient.DeleteObject(delReq);
                    }
                }

                // Testing database server
                conn.ChangeDatabase("master");
                conn.Execute("drop database " + dbName);

                if (File.Exists(backupFile))
                {
                    File.Delete(backupFile);
                }
            }
        }
    }
}
