SQL Server AWS Backup Tool
=
## (BETA)
### A simple tool that backups a single SQL Server database and puts it on S3

This tool backups a single database on SQL Server, takes that
backup, and writes it to S3. No compression or other fancy tools
are present. It takes a full backup and does not perform log or
differential backups. The only fancy feature is that it will delete
backups that are older than X number of days, where X is the
number you set the variable retention_policy to.

## Usage

To use, you need to create an INI file with these variables filled in:

    [sqlserver]  
    server=  
    database=  
    temp_dir=C:\temp\  

    [aws]  
    access_key =  
    secret_key =  
    bucket =  
    profile = 

    [general]  
    retention_policy=14  

The variable retention_policy is in days. Then, you pass the path to the file as the first argument to the program:

    SqlServerAwsBackupTool.exe config.ini

## Deployment

- Set build to release mode and build
- Add any new/updated configuration variables
- Add/change any scheduled tasks
- Verify that your backups are occuring; test them by restoring them (on a test server)

## Testing your backups

Please verify that your process works from end to end. I have tested this program
but you must do your own testing.

## AWS Credentials

This requries that you have AWS credentials with read/write access
to S3.

## Testing

For testing, you need to create a config called C:\SqlServerAwsBackupTool\config.ini.

This should have the same information as the above config,
but point it to your development machine and development AWS environment.