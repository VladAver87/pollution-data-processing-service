package com.vladaver.data_processing.liquibase

import com.vladaver.data_processing.utils.Utils.AppConfig
import liquibase.Liquibase
import liquibase.database.DatabaseFactory
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.ClassLoaderResourceAccessor
import org.postgresql.ds.PGSimpleDataSource

import java.sql.Connection

class SchemaMigration(config: AppConfig)  {
  val masterChangeLogFile = "classpath:changelog-master.xml"

  def createLiquibase(dbConnection: Connection, diffFilePath: String): Liquibase = {
    val database = DatabaseFactory.getInstance.findCorrectDatabaseImplementation(new JdbcConnection(dbConnection))
    val classLoader = classOf[SchemaMigration].getClassLoader
    val resourceAccessor = new ClassLoaderResourceAccessor(classLoader)
    new Liquibase(diffFilePath, resourceAccessor, database)
  }

  def updateDb(db: PGSimpleDataSource, diffFilePath: String): Unit = {
    val dbConnection = db.getConnection
    val liquibase = createLiquibase(dbConnection, diffFilePath)
    liquibase.clearCheckSums()
    try {
      liquibase.update(null)
    } catch {
      case e: Throwable => throw e
    } finally {
      liquibase.forceReleaseLocks()
      dbConnection.rollback()
      dbConnection.close()
    }
  }

  def getConnectionProvider(): PGSimpleDataSource = {
    val datasource = new PGSimpleDataSource()
    datasource.setUrl(config.pgUrl)
    datasource.setUser(config.pgUsername)
    datasource.setPassword(config.pgPassword)
    datasource
  }

  def run(): Unit = {
    val db = getConnectionProvider()
    updateDb(db, masterChangeLogFile)
  }

}


