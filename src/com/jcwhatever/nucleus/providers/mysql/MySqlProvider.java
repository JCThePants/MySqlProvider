package com.jcwhatever.nucleus.providers.mysql;

import com.jcwhatever.nucleus.providers.Provider;
import com.jcwhatever.nucleus.providers.mysql.data.ItemStacksDataType;
import com.jcwhatever.nucleus.providers.mysql.data.LocationDataType;
import com.jcwhatever.nucleus.providers.mysql.data.TimeStampDataType;
import com.jcwhatever.nucleus.providers.mysql.data.UUIDDataType;
import com.jcwhatever.nucleus.providers.mysql.data.VectorDataType;
import com.jcwhatever.nucleus.providers.mysql.data.bytes.Binary;
import com.jcwhatever.nucleus.providers.mysql.data.bytes.Char;
import com.jcwhatever.nucleus.providers.mysql.data.bytes.VarBinary;
import com.jcwhatever.nucleus.providers.mysql.data.bytes.VarChar;
import com.jcwhatever.nucleus.providers.mysql.data.numeric.BigIntSigned;
import com.jcwhatever.nucleus.providers.mysql.data.numeric.BigIntUnsigned;
import com.jcwhatever.nucleus.providers.mysql.data.numeric.Bit;
import com.jcwhatever.nucleus.providers.mysql.data.numeric.DecimalDataType;
import com.jcwhatever.nucleus.providers.mysql.data.numeric.DoubleDataType;
import com.jcwhatever.nucleus.providers.mysql.data.numeric.FloatDataType;
import com.jcwhatever.nucleus.providers.mysql.data.numeric.IntSigned;
import com.jcwhatever.nucleus.providers.mysql.data.numeric.IntUnsigned;
import com.jcwhatever.nucleus.providers.mysql.data.numeric.MediumIntSigned;
import com.jcwhatever.nucleus.providers.mysql.data.numeric.MediumIntUnsigned;
import com.jcwhatever.nucleus.providers.mysql.data.numeric.SmallIntSigned;
import com.jcwhatever.nucleus.providers.mysql.data.numeric.SmallIntUnsigned;
import com.jcwhatever.nucleus.providers.mysql.data.numeric.TinyIntSigned;
import com.jcwhatever.nucleus.providers.mysql.data.numeric.TinyIntUnsigned;
import com.jcwhatever.nucleus.providers.mysql.datanode.SqlDataNodeBuilder;
import com.jcwhatever.nucleus.providers.mysql.statements.FinalizedStatements;
import com.jcwhatever.nucleus.providers.mysql.statements.StatementExecutor;
import com.jcwhatever.nucleus.providers.mysql.statements.Transaction;
import com.jcwhatever.nucleus.providers.sql.ISqlDatabase;
import com.jcwhatever.nucleus.providers.sql.ISqlDbType;
import com.jcwhatever.nucleus.providers.sql.ISqlProvider;
import com.jcwhatever.nucleus.providers.sql.ISqlQueryResult;
import com.jcwhatever.nucleus.providers.sql.ISqlResult;
import com.jcwhatever.nucleus.providers.sql.datanode.ISqlDataNodeBuilder;
import com.jcwhatever.nucleus.utils.PreCon;
import com.jcwhatever.nucleus.utils.observer.future.FutureResultAgent;
import com.jcwhatever.nucleus.utils.observer.future.IFuture;
import com.jcwhatever.nucleus.utils.observer.future.IFutureResult;

import org.bukkit.plugin.Plugin;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implementation of {@link ISqlProvider}.
 */
public class MySqlProvider extends Provider implements ISqlProvider {

    private static MySqlProvider _instance;
    private static final Map<String, ISqlDbType> _dataTypesByName = new HashMap<>(35);
    private static final Map<Class<?>, ISqlDbType> _primitiveDataTypesSigned = new HashMap<>(7);
    private static final Map<Class<?>, ISqlDbType> _primitiveDataTypesUnsigned = new HashMap<>(7);
    private static final Map<Class<?>, ISqlDbType> _dataTypesByClass = new HashMap<>(35);

    private static ISqlDbType add(ISqlDbType type) {
        _dataTypesByName.put(type.getName(), type);

        boolean isPrimitive = type.getDataClass().isPrimitive();

        if (!(isPrimitive && !type.isSigned()))
            _dataTypesByClass.put(type.getDataClass(), type);

        if (isPrimitive && type.isSigned()) {
            _primitiveDataTypesSigned.put(type.getDataClass(), type);
        }
        else if (isPrimitive) {
            _primitiveDataTypesUnsigned.put(type.getDataClass(), type);
        }

        return type;
    }

    public static final ISqlDbType BOOLEAN = add(new Bit());
    public static final ISqlDbType BYTE_SIGNED = add(new TinyIntSigned());
    public static final ISqlDbType BYTE_UNSIGNED = add(new TinyIntUnsigned());
    public static final ISqlDbType SHORT_SIGNED = add(new SmallIntSigned());
    public static final ISqlDbType SHORT_UNSIGNED = add(new SmallIntUnsigned());
    public static final ISqlDbType MEDIUM_INT_SIGNED = add(new MediumIntSigned());
    public static final ISqlDbType MEDIUM_INT_UNSIGNED = add(new MediumIntUnsigned());
    public static final ISqlDbType INTEGER_SIGNED = add(new IntSigned());
    public static final ISqlDbType INTEGER_UNSIGNED = add(new IntUnsigned());
    public static final ISqlDbType LONG_SIGNED = add(new BigIntSigned());
    public static final ISqlDbType LONG_UNSIGNED = add(new BigIntUnsigned());
    public static final ISqlDbType FLOAT = add(new FloatDataType());
    public static final ISqlDbType DOUBLE = add(new DoubleDataType());
    public static final ISqlDbType BIG_DECIMAL = add(new DecimalDataType());
    public static final ISqlDbType DATE = add(new TimeStampDataType());
    public static final ISqlDbType LOCATION = add(new LocationDataType());
    public static final ISqlDbType VECTOR = add(new VectorDataType());
    public static final ISqlDbType UUID = add(new UUIDDataType());
    public static final ISqlDbType ITEM_STACKS = add(new ItemStacksDataType());

    /**
     * Get the provider instance.
     */
    public static MySqlProvider getProvider() {
        return _instance;
    }

    private StatementExecutor _statementExecutor;

    /**
     * Constructor.
     */
    public MySqlProvider() {
        super();

        try {
            Class.forName ("com.mysql.jdbc.Driver").newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException("JDBC database driver not found.");
        }
    }

    @Override
    public ISqlDbType getDataType(Class<?> typeClazz) {
        PreCon.notNull(typeClazz);

        return _dataTypesByClass.get(typeClazz);
    }

    @Nullable
    @Override
    public ISqlDbType getDataType(Class<?> typeClazz, boolean isSigned) {
        PreCon.notNull(typeClazz);

        if (typeClazz.isPrimitive()) {

            return isSigned
                    ? _primitiveDataTypesSigned.get(typeClazz)
                    : _primitiveDataTypesUnsigned.get(typeClazz);
        }

        return getDataType(typeClazz);
    }

    @Override
    public ISqlDbType getDataType(String name) {
        PreCon.notNullOrEmpty(name);

        return _dataTypesByName.get(name);
    }

    @Nullable
    @Override
    public ISqlDbType getDataType(String typeName, boolean isSigned) {
        PreCon.notNullOrEmpty(typeName);

        if (typeName.endsWith(" UNSIGNED"))
            return getDataType(typeName);

        return isSigned ? getDataType(typeName) : getDataType(typeName + " UNSIGNED");
    }

    @Override
    public Collection<ISqlDbType> getDataTypes() {
        return new ArrayList<>(_dataTypesByName.values());
    }

    @Override
    public String getDatabaseBrand() {
        return ISqlProvider.MYSQL;
    }

    @Override
    public IFutureResult<ISqlDatabase> connect(
            String address, String databaseName, String userName, String password) {

        PreCon.notNullOrEmpty(address);
        PreCon.notNull(databaseName);
        PreCon.notNull(userName);
        PreCon.notNull(password);

        Database database = new Database(address, databaseName, userName, password);

        return new FutureResultAgent<ISqlDatabase>().success(database);
    }

    public IFutureResult<ISqlResult> execute(Transaction transaction, FutureResultAgent<ISqlResult> agent) {
        PreCon.notNull(transaction);

        _statementExecutor.execute(transaction, agent);

        return agent.getFuture();
    }

    public IFutureResult<ISqlResult> execute(FinalizedStatements statements) {
        PreCon.notNull(statements);

        FutureResultAgent<ISqlResult> agent = new FutureResultAgent<>();

        _statementExecutor.execute(statements, agent);

        return agent.getFuture();
    }

    @Override
    public IFutureResult<ISqlQueryResult> executeQuery(PreparedStatement statement) {
        return null; // TODO
    }

    @Override
    public IFuture execute(PreparedStatement statement) {
        return null; // TODO
    }

    @Override
    public ISqlDataNodeBuilder createDataNodeBuilder(Plugin plugin) {
        PreCon.notNull(plugin);

        return new SqlDataNodeBuilder(plugin);
    }

    @Override
    protected void onEnable() {
        _instance = this;
        _statementExecutor = new StatementExecutor(4);
    }

    @Override
    protected void onDisable() {
        _statementExecutor.dispose();
        _instance = null;
    }

    @Override
    public ISqlDbType getBoolean() {
        return BOOLEAN;
    }

    @Override
    public ISqlDbType getUnsignedByte() {
        return BYTE_UNSIGNED;
    }

    @Override
    public ISqlDbType getUnsignedShort() {
        return SHORT_UNSIGNED;
    }

    @Override
    public ISqlDbType getUnsignedMediumInteger() {
        return MEDIUM_INT_UNSIGNED;
    }

    @Override
    public ISqlDbType getUnsignedInteger() {
        return INTEGER_UNSIGNED;
    }

    @Override
    public ISqlDbType getUnsignedLong() {
        return LONG_UNSIGNED;
    }

    @Override
    public ISqlDbType getSignedByte() {
        return BYTE_SIGNED;
    }

    @Override
    public ISqlDbType getSignedShort() {
        return SHORT_SIGNED;
    }

    @Override
    public ISqlDbType getSignedMediumInteger() {
        return MEDIUM_INT_SIGNED;
    }

    @Override
    public ISqlDbType getSignedInteger() {
        return INTEGER_SIGNED;
    }

    @Override
    public ISqlDbType getSignedLong() {
        return LONG_SIGNED;
    }

    @Override
    public ISqlDbType getFloat() {
        return FLOAT;
    }

    @Override
    public ISqlDbType getDouble() {
        return DOUBLE;
    }

    @Override
    public ISqlDbType getBigDecimal() {
        return BIG_DECIMAL;
    }

    @Override
    public ISqlDbType getDate() {
        return DATE;
    }

    @Override
    public ISqlDbType getLocation() {
        return LOCATION;
    }

    @Override
    public ISqlDbType getVector() {
        return VECTOR;
    }

    @Override
    public ISqlDbType getUUID() {
        return UUID;
    }

    @Override
    public ISqlDbType getItemStacks() {
        return ITEM_STACKS;
    }

    @Override
    public ISqlDbType getFixedString(int size) {
        return new Char(size);
    }

    @Override
    public ISqlDbType getString(int size) {
        return new VarChar(size);
    }

    @Override
    public ISqlDbType getFixedByteArray(int size) {
        return new Binary(size);
    }

    @Override
    public ISqlDbType getByteArray(int size) {
        return new VarBinary(size);
    }
}
