# athena-jdbc

这是Athena JDBC driver

fork自https://github.com/burtcorp/athena-jdbc后，添加了一些功能

我们可以通过Athena JDBC driver来访问亚马逊AWS Athena数据。

也可以在一些SQL workbench里例如(DataGrip, Dbeaver, SqlWorkBench)使用Athena JDBC driver来访问

当前，有以下几种Athena JDBC driver:
1. 亚马逊官方的AWS Athena JDBC driver: https://github.com/burtcorp/athena-jdbc
2. DataGrip, Dbeaver有自带的Athena JDBC驱动
3. github上开源的： https://github.com/burtcorp/athena-jdbc

我尝试了下，使用Datagrip自带的和亚马逊官方的Athena JDBC驱动，都没能某种特殊的网络环境条件下，在Datagrip和SqlWorkBench连接成功。

后来发现github上有athena-jdbc的code: https://github.com/burtcorp/athena-jdbc

测试了下发现可以执行sql query,但是放到Datagrip和SqlWorkBench却用不了

于是研究了在Datagrip和SqlWorkBench里调试JDBC驱动，发现最主要的是DatabaseMetaData类里少了最主要的几个关键函数

把这几个关键函数加上就能用了。前期主要在SqlWorkBench里调试，后来发现DataGrip更好用，所以后来放弃了SqlWorkBench的支持

要是想支持的话，稍微改改应该也行. 





This is a JDBC driver for AWS Athena.

update: 20221123 version: 0.5.1
1. support using AWS profile. 

Usage: add property in advance tab: profile, and then no need Authentication
2. optimize catalog
3. query timeout set to 120 seconds

update: 20221122:
support used in DataGrip

