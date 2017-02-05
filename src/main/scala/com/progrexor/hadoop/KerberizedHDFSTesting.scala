package com.progrexor.hadoop

import java.io.File
import java.nio.file.Files

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.{DFSConfigKeys, HdfsConfiguration, MiniDFSCluster}
import org.apache.hadoop.http.HttpConfig
import org.apache.hadoop.minikdc.MiniKdc
import org.apache.hadoop.security.ssl.KeyStoreTestUtil
import org.apache.log4j.{Level, Logger}

/**
  * Created by andreyd on 05/02/2017.
  */
object KerberizedHDFSTesting {
  def main(args: Array[String]): Unit = {

    // Logger
    val logger = Logger.getRootLogger
    logger.setLevel(Level.ERROR)

    // Set Kerberos
    val userName = System.getenv("USER")
    val baseDir = Files.createTempDirectory("kdc_temp").toFile
    val kdcConf = MiniKdc.createConf
    val kdc = new MiniKdc(kdcConf, baseDir)
    kdc.start()
    val keytabFile = new File(baseDir, s"$userName.keytab")
    val keytab = keytabFile.getAbsolutePath
    kdc.createPrincipal(keytabFile, s"$userName/localhost")
    val hdfsPrincipal = s"$userName/localhost@" + kdc.getRealm

    // Set HDFS Configuration
    val conf = new HdfsConfiguration
    conf.set("hadoop.security.authentication", "kerberos")
    conf.set(DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal)
    conf.set(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY, keytab)
    conf.set(DFSConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication")
    conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name)
    conf.set(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal)
    conf.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, keytab)
    conf.set(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal)
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true)

    // Set SSL
    val keystoresDir = baseDir.getAbsolutePath
    val sslConfDir = KeyStoreTestUtil.getClasspathDir(this.getClass)
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false)

    // Set HDFS cluster
    val cluster = new MiniDFSCluster.Builder(conf).build
    cluster.waitActive()

    // Test HDFS operation
    val fs = cluster.getFileSystem
    val list = (path: String) => fs.listStatus(new Path(path)).map(_.getPath.toString).toList
    fs.mkdirs(new Path("MyDir"))
    list(".").foreach(println)

    // Free resources
    cluster.shutdown()
    kdc.stop()
  }
}
