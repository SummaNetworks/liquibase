package liquibase.change.core;

import java.util.LinkedList;
import java.util.List;

import liquibase.change.custom.CustomSqlChange;
import liquibase.database.Database;
import liquibase.exception.CustomChangeException;
import liquibase.exception.DatabaseException;
import liquibase.exception.SetupException;
import liquibase.exception.ValidationErrors;
import liquibase.executor.ExecutorService;
import liquibase.executor.jvm.JdbcExecutor;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;


public class CustomSplitUpdateSQLChange implements CustomSqlChange {

    private static final String MIN_LIMIT = "minLimit";
    private static final String MAX_LIMIT = "maxLimit";
    private static final String ITEMS_PER_COMMIT = "itemsPerCommit";
    private Integer maxNumberOfItems;
    private Integer itemsPerCommit;
    private Integer sleepInSeconds;
    private String updateQuery;
    private String countQuery;

    @SuppressWarnings({"UnusedDeclaration", "FieldCanBeLocal"})
    private ResourceAccessor resourceAccessor;

    public CustomSplitUpdateSQLChange() {
    }

    public CustomSplitUpdateSQLChange(String updateQuery, String rollbackQuery, String countQuery, Integer maxNumberOfItems, Integer itemsPerCommit, Integer sleepInSeconds) {
        this.countQuery = countQuery;
        this.maxNumberOfItems = maxNumberOfItems;
        this.itemsPerCommit = itemsPerCommit;
        this.updateQuery = updateQuery;
        this.sleepInSeconds = sleepInSeconds;
    }

    public String getCountQuery() {
        return countQuery;
    }

    public void setCountQuery(String countQuery) {
        this.countQuery = countQuery;
    }

    public Integer getMaxNumberOfItems() {
        return maxNumberOfItems;
    }

    public void setMaxNumberOfItems(Integer maxNumberOfItems) {
        this.maxNumberOfItems = maxNumberOfItems;
    }

    public Integer getItemsPerCommit() {
        return itemsPerCommit;
    }

    public void setItemsPerCommit(Integer itemsPerCommit) {
        this.itemsPerCommit = itemsPerCommit;
    }

    public ResourceAccessor getResourceAccessor() {
        return resourceAccessor;
    }

    public void setResourceAccessor(ResourceAccessor resourceAccessor) {
        this.resourceAccessor = resourceAccessor;
    }

    public String getUpdateQuery() {
        return updateQuery;
    }

    public void setUpdateQuery(String updateQuery) {
        this.updateQuery = updateQuery;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) throws CustomChangeException {
        Integer total;

        if (maxNumberOfItems == null) {
            JdbcExecutor writeExecutor = new JdbcExecutor();
            writeExecutor.setDatabase(database);
            ExecutorService.getInstance().setExecutor(database, writeExecutor);
            try {
                total = writeExecutor.queryForInt(new RawSqlStatement(countQuery));
            } catch (DatabaseException e) {
                throw new CustomChangeException(e);
            }

        } else {
            total = maxNumberOfItems;
        }

        List<SqlStatement> result = new LinkedList<SqlStatement>();
        Integer currentIndex =  0;
        while (currentIndex < total){
            String finalQuery = updateQuery.replace(MIN_LIMIT, String.valueOf(currentIndex));
            finalQuery = finalQuery.replace(MAX_LIMIT, String.valueOf(currentIndex + itemsPerCommit));
            result.add(new RawSqlStatement(finalQuery));
            result.add(new RawSqlStatement("commit;"));
            result.add(new RawSqlStatement("SELECT SLEEP("+ sleepInSeconds + ");"));
            currentIndex = currentIndex + itemsPerCommit;
        }

        return result.toArray(new SqlStatement[]{});
    }

    @Override
    public String getConfirmationMessage() {
        return "Split class updated with values " + updateQuery + ", " + maxNumberOfItems + ", " + itemsPerCommit;
    }

    @Override
    public void setUp() throws SetupException {
    }

    @Override
    public void setFileOpener(ResourceAccessor resourceAccessor) {
        this.resourceAccessor = resourceAccessor;
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();

        if (countQuery==null && maxNumberOfItems == null) {
            validationErrors.addError("countQuery or maxNumberOfItems  is required");
        }
        if (updateQuery==null){
            validationErrors.addError("updateQuery is required");
        } else {
            if (!updateQuery.contains(MIN_LIMIT)){
                validationErrors.addError("'" + MIN_LIMIT + "' text is required in updateQuery text");
            }
            if (!updateQuery.contains(MAX_LIMIT)){
                validationErrors.addError("'" + MAX_LIMIT + "' text is required in updateQuery text");
            }
        }

        if (itemsPerCommit == null) {
            validationErrors.addError("'" + ITEMS_PER_COMMIT + "' is required");
        }
        return validationErrors;
    }

    public Integer getSleepInSeconds() {
        return sleepInSeconds;
    }

    public void setSleepInSeconds(Integer sleepInSeconds) {
        this.sleepInSeconds = sleepInSeconds;
    }
}
