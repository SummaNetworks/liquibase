package liquibase.change.custom;

import liquibase.change.core.CustomSplitUpdateSQLChange;
import liquibase.database.Database;
import liquibase.exception.CustomChangeException;
import liquibase.exception.ValidationErrors;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

public class CustomSplitUpdateSQLChangeTest {

    @Test
    public void constructor() {
        CustomSplitUpdateSQLChange change = new CustomSplitUpdateSQLChange();
    }

    @Test
    public void validate() {
        CustomSplitUpdateSQLChange change = new CustomSplitUpdateSQLChange();
        ValidationErrors validate = change.validate(mock(Database.class));
        assertNotEquals(validate.getErrorMessages().size(), 0);

        change = new CustomSplitUpdateSQLChange(
                "UPDATE Sim SET sgsnVolatileData_id = hlrVolatileData_id, vlrVolatileData_id = hlrVolatileData_id\n" +
                        "            where hlrVolatileData_id >=minLimit and hlrVolatileData_id&lt;maxLimit;", "UPDATE Sim SET sgsnVolatileData_id = null, vlrVolatileData_id = null\n" +
                                "                where hlrVolatileData_id >=minLimit and hlrVolatileData_id&lt;maxLimit;", null, 5000000,
                200000, 10);
        validate = change.validate(mock(Database.class));
        assertEquals(validate.getErrorMessages().size(), 0);
    }

    @Test
    public void generateStatements() throws CustomChangeException {
        CustomSplitUpdateSQLChange change = new CustomSplitUpdateSQLChange(
                "UPDATE Sim SET sgsnVolatileData_id = hlrVolatileData_id where hlrVolatileData_id >=minLimit " +
                        "and hlrVolatileData_id&lt;maxLimit;", "UPDATE Sim SET sgsnVolatileData_id = null " +
                "where hlrVolatileData_id >=minLimit and hlrVolatileData_id&lt;maxLimit;",null , 10,
                10, 10);


        SqlStatement[] statements = change.generateStatements(mock(Database.class));
        assertEquals(9, statements.length);
        assertEquals("UPDATE Sim SET sgsnVolatileData_id = hlrVolatileData_id where hlrVolatileData_id >=0 " +
                "and hlrVolatileData_id&lt;5;", ((RawSqlStatement) statements[0]).getSql());
        assertEquals("commit;", ((RawSqlStatement) statements[1]).getSql());
        assertEquals("SELECT SLEEP(10);", ((RawSqlStatement) statements[2]).getSql());
        assertEquals("UPDATE Sim SET sgsnVolatileData_id = hlrVolatileData_id where hlrVolatileData_id >=5 " +
                "and hlrVolatileData_id&lt;10;", ((RawSqlStatement) statements[3]).getSql());
        assertEquals("commit;", ((RawSqlStatement) statements[4]).getSql());
        assertEquals("SELECT SLEEP(10);", ((RawSqlStatement) statements[5]).getSql());
    }

/*
    @DatabaseChange(name = "CustomSplitUpdateSQLChange", description = "Used for the AbstractSQLChangeTest unit test", priority = 1)
    private static class CustomSplitUpdateSQLChange extends AbstractSQLChange {

        private CustomSplitUpdateSQLChange() {
        }

        private CustomSplitUpdateSQLChange(String sql) {
            setSql(sql);
        }


        @Override
        public String getConfirmationMessage() {
            return "Example SQL Change Message";
        }

        @Override
        public String getSerializedObjectNamespace() {
            return STANDARD_CHANGELOG_NAMESPACE;
        }

    }*/
}
