package org.h2.benchmarks;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 *
 */
@State(Scope.Thread)
public class PrepareView {
    private Connection c;

    @Setup
    public void setup() throws SQLException {
        c = DriverManager.getConnection("jdbc:h2:mem:test;QUERY_CACHE_SIZE=0");
        Statement s = c.createStatement();
        s.execute("create table t(id int primary key, name varchar unique)");
        s.execute("create view v as select 1 as one, id, name from t");
        s.close();
    }

    @TearDown
    public void tearDown() throws SQLException {
        c.close();
    }

    @Benchmark
    @Fork(10)
    @BenchmarkMode(Mode.Throughput)
    public PreparedStatement testView() throws SQLException {
        return c.prepareStatement("select one from v where id = 1 and name = 'Ivan'");
    }

    @Benchmark
    @Fork(10)
    @BenchmarkMode(Mode.Throughput)
    public PreparedStatement testSubQuery() throws SQLException {
        return c.prepareStatement("select one from (select 1 as one, id, name from t) where id = 1 and name = 'Ivan'");
    }

}
