### Description
- This is a database synchronization tool built with Golang, specifically designed for synchronizing MySQL databases. The tool captures data changes based on MySQL's binary log (binlog) and simulates a MySQL slave to achieve real-time database synchronization. This approach allows the tool to non-intrusively listen to and parse the binlog, tracking insert, update, and delete operations in the database, and then synchronizing these changes to the target database or other systems.

### Configuration
<code>

    Mysql:
      Host: 192.168.2.204
      Port: 30306
      User: root
      Pwd: 123456

    Slave:
      ServerID: 1000   //ID
      Flavor: mysql   
      Binlog: mysql-bin.000002 //binlog-name
      Pos: 1  //start position
      LoadNew: false  //is load new
      LoadPath: "/Users/liu/Downloads/a1/"
</code>
