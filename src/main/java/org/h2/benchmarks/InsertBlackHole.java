/*
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.h2.benchmarks;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import org.h2.api.TableEngine;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.result.Row;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableBase;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Thread)
public class InsertBlackHole {

    Connection c;
    PreparedStatement insert;

    @Setup
    public void setup(Blackhole blackhole) throws SQLException {
        c = DriverManager.getConnection("jdbc:h2:mem:test");
        c.setAutoCommit(true);
        BlackHoleTable.blackhole = blackhole;
        c.createStatement().execute("create table t(id int) engine \"" + BlackHoleEngine.class.getName() + "\"");
        insert = c.prepareStatement("insert into t values (1),(2),(3),(4),(5)");
    }

    @TearDown
    public void tearDown() throws SQLException {
        insert.close();
        c.close();
    }

    @Benchmark
    @Fork(10)
    @BenchmarkMode(Mode.Throughput)
    public int testInsert() throws SQLException {
        return insert.executeUpdate();
    }

    public static class BlackHoleEngine implements TableEngine {
        @Override
        public Table createTable(CreateTableData createTableData) {
            return new BlackHoleTable(createTableData);
        }
    }

    private static class BlackHoleTable extends TableBase {
        static Blackhole blackhole;

        public BlackHoleTable(CreateTableData createTableData) {
            super(createTableData);
        }

        @Override
        public boolean lock(Session session, boolean b, boolean b1) {
            return false;
        }

        @Override
        public void close(Session session) {
            // No-op.
        }

        @Override
        public void unlock(Session session) {
            // No-op.
        }

        @Override
        public Index addIndex(Session session, String s, int i, IndexColumn[] indexColumns, IndexType indexType,
            boolean b,
            String s1) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeRow(Session session, Row row) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void truncate(Session session) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addRow(Session session, Row row) {
            blackhole.consume(row);
        }

        @Override
        public void checkSupportAlter() {
            // No-op.
        }

        @Override
        public String getTableType() {
            return EXTERNAL_TABLE_ENGINE;
        }

        @Override
        public Index getScanIndex(Session session) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Index getUniqueIndex() {
            return null;
        }

        @Override
        public ArrayList<Index> getIndexes() {
            return null;
        }

        @Override
        public boolean isLockedExclusively() {
            return false;
        }

        @Override
        public long getMaxDataModificationId() {
            return 0;
        }

        @Override
        public boolean isDeterministic() {
            return false;
        }

        @Override
        public boolean canGetRowCount() {
            return true;
        }

        @Override
        public boolean canDrop() {
            return true;
        }

        @Override
        public long getRowCount(Session session) {
            return 0;
        }

        @Override
        public long getRowCountApproximation() {
            return 0;
        }

        @Override
        public long getDiskSpaceUsed() {
            return 0;
        }

        @Override
        public void checkRename() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isMVStore() {
            return true;
        }
    }
}
