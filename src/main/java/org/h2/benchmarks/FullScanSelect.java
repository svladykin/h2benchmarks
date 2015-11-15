package org.h2.benchmarks;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Thread)
public class FullScanSelect {
    Connection c;
    PreparedStatement select;

    @Setup
    public void setup() throws SQLException {
        c = DriverManager.getConnection("jdbc:h2:mem:test;OPTIMIZE_REUSE_RESULTS=0");
        c.setAutoCommit(true);
        c.createStatement().execute("create table t(i0 int, i1 int, i2 int, i3 int, i4 int, i5 int, i6 int, i7 int)");
        PreparedStatement insert = c.prepareStatement("insert into t values (?,?,?,?,?,?,?,?)");
        for (int i = 0; i < 1000; i++) {
            for (int j = 1; j <= 8; j++) {
                insert.setInt(j, i);
            }
            insert.executeUpdate();
        }
        insert.close();
        select = c.prepareStatement("select * from t where i0 <> i1 or i2 <> i3 or i4 <> i5 or i6 <> i7");
    }

    @TearDown
    public void tearDown() throws SQLException {
        select.close();
        c.close();
    }

    @Benchmark
    @Fork(10)
    @BenchmarkMode(Mode.Throughput)
    public boolean testSelect() throws SQLException {
        return select.executeQuery().next();
    }
}
