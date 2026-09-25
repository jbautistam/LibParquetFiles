using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Parquet;
using Parquet.Schema;

namespace Bau.Libraries.LibParquetFiles.Readers;

/// <summary>
///		Implementación de <see cref="System.Data.IDataReader"/> para archivos Parquet
/// </summary>
public class ParquetDataReader : System.Data.IDataReader
{
	// Eventos públicos
	public event EventHandler<EventArguments.AffectedEvntArgs>? Progress;
	// Variables privadas
	private Stream? _fileReader;
	private ParquetReader? _parquetReader;
	private DataField[] _schema = default!;
	private object?[][]? _groupRowColumns;
	private int _rowsInGroup;
	private int _rowGroup = 0, _actualRow = 0;
	private object?[] _rowValues = default!;
	private long _row;

	public ParquetDataReader(int notifyAfter = 10_000)
	{
		NotifyAfter = notifyAfter;
	}

	/// <summary>
	///		Abre el archivo
	/// </summary>
	public async Task OpenAsync(string fileName, CancellationToken cancellationToken)
	{
		await OpenAsync(File.OpenRead(fileName), cancellationToken);
	}

	/// <summary>
	///		Abre el archivo
	/// </summary>
	public async Task OpenAsync(Stream stream, CancellationToken cancellationToken)
	{
		// Si ya había un archivo abierto con esta misma instancia, lo cierra primero (evita fugas y esquemas obsoletos)
		if (!IsClosed)
			Close();
		// Asigna el stream al archivo
		try
		{
			_fileReader = stream;
			_parquetReader = await ParquetReader.CreateAsync(_fileReader, cancellationToken: cancellationToken);
		}
		catch
		{
			// Si falla la apertura, no deja el archivo bloqueado
			_fileReader?.Close();
			_fileReader = null;
			_parquetReader = null;
			throw;
		}
		// Reinicia el estado de lectura (incluido el esquema, que podría ser de un archivo anterior)
		_row = 0;
		_rowGroup = 0;
		_actualRow = 0;
		_rowsInGroup = 0;
		_schema = default!;
		_groupRowColumns = null;
		_rowValues = default!;
		// Indica que está abierto
		IsClosed = false;
	}

	/// <summary>
	///		Lee un registro (de forma síncrona) [necesario para implementar el interface <see cref="DbData.IDataReader"/>
	/// </summary>
	public bool Read()
	{
		return Task.Run(async () => await ReadAsync(CancellationToken.None)).GetAwaiter().GetResult();
	}

	/// <summary>
	///		Lee un registro
	/// </summary>
	public async Task<bool> ReadAsync(CancellationToken cancellationToken)
	{
		bool readed = false;

			// Si realmente hay algo que leer
			if (_parquetReader != null)
			{
				// Recorre los grupos de filas del archivo (salta los grupos vacíos)
				while (_groupRowColumns is null || _actualRow >= _rowsInGroup)
				{
					// Si no quedan más grupos, deja de intentar leer
					if (_rowGroup >= _parquetReader.RowGroupCount)
					{
						_groupRowColumns = null;
						_rowsInGroup = 0;
						break;
					}
					// Interpreta el esquema si aún no se ha leido
					if (_schema is null || _schema.Length == 0)
						ParseSchema();
					if (_schema is null)
						throw new InvalidOperationException("Can't read the schema");
					// Obtiene el lector con el grupo de filas y lee sus columnas
					using (ParquetRowGroupReader groupReader = _parquetReader.OpenRowGroupReader(_rowGroup))
					{
						// Obtiene el número de filas del grupo
						_rowsInGroup = (int) groupReader.RowCount;
						// Lee las columnas del grupo
						_groupRowColumns = new object?[_schema.Length][];
						for (int index = 0; index < _schema.Length; index++)
							_groupRowColumns[index] = await ReadColumnAsync(groupReader, _schema[index], _rowsInGroup, cancellationToken);
					}
					// Incrementa el número de grupo y cambia la fila actual
					_rowGroup++;
					_actualRow = 0;
				}
				// Obtiene los datos (si queda algo por leer)
				if (_groupRowColumns != null)
				{
					// Transforma las columnas
					_rowValues = new object?[_groupRowColumns.Length];
					for (int index = 0; index < _groupRowColumns.Length; index++)
						_rowValues[index] = _groupRowColumns[index][_actualRow];
					// Indica que se ha leido el registro e incrementa la fila actual
					readed = true;
					_actualRow++;
					// Incrementa la fila total y lanza el evento (RaiseEventReadBlock ya comprueba que NotifyAfter sea positivo)
					_row++;
					RaiseEventReadBlock(_row);
				}
			}
			// Devuelve el valor que indica si se ha leido un registro
			return readed;
	}

	/// <summary>
	///		Interpreta el esquema del archivos
	/// </summary>
	private void ParseSchema()
	{
		if (_parquetReader != null)
			_schema = _parquetReader.Schema.GetDataFields();
	}

	/// <summary>
	///		Se asegura de que el esquema se ha interpretado; lanza una excepción si el archivo no está abierto
	/// </summary>
	[MemberNotNull(nameof(_schema))]
	private void EnsureSchema()
	{
		if (_schema is null || _schema.Length == 0)
			ParseSchema();
		if (_schema is null)
			throw new InvalidOperationException("The reader is not open: call OpenAsync first");
	}

	/// <summary>
	///		Se asegura de que hay una fila actual leida; lanza una excepción si no se ha llamado antes a Read()
	/// </summary>
	private object?[] EnsureCurrentRow()
	{
		if (_rowValues is null)
			throw new InvalidOperationException("There is no current row: call Read() first");
		return _rowValues;
	}

	/// <summary>
	///		Lee una columna de un grupo de filas y devuelve sus valores ya convertidos a <see cref="object"/>
	/// </summary>
	private static async Task<object?[]> ReadColumnAsync(ParquetRowGroupReader groupReader, DataField field, int rows, CancellationToken cancellationToken)
	{
		Type type = field.ClrType;

			if (type == typeof(string))
			{
				string?[] values = new string?[rows];

					await groupReader.ReadAsync(field, values.AsMemory(), cancellationToken: cancellationToken);
					return values;
			}
			else if (type == typeof(byte[]))
			{
				byte[]?[] values = new byte[]?[rows];

					await groupReader.ReadAsync(field, values.AsMemory(), cancellationToken: cancellationToken);
					return values;
			}
			else if (type == typeof(bool))
				return await ReadStructColumnAsync<bool>(groupReader, field, rows, cancellationToken);
			else if (type == typeof(byte))
				return await ReadStructColumnAsync<byte>(groupReader, field, rows, cancellationToken);
			else if (type == typeof(sbyte))
				return await ReadStructColumnAsync<sbyte>(groupReader, field, rows, cancellationToken);
			else if (type == typeof(short))
				return await ReadStructColumnAsync<short>(groupReader, field, rows, cancellationToken);
			else if (type == typeof(ushort))
				return await ReadStructColumnAsync<ushort>(groupReader, field, rows, cancellationToken);
			else if (type == typeof(int))
				return await ReadStructColumnAsync<int>(groupReader, field, rows, cancellationToken);
			else if (type == typeof(uint))
				return await ReadStructColumnAsync<uint>(groupReader, field, rows, cancellationToken);
			else if (type == typeof(long))
				return await ReadStructColumnAsync<long>(groupReader, field, rows, cancellationToken);
			else if (type == typeof(ulong))
				return await ReadStructColumnAsync<ulong>(groupReader, field, rows, cancellationToken);
			else if (type == typeof(float))
				return await ReadStructColumnAsync<float>(groupReader, field, rows, cancellationToken);
			else if (type == typeof(double))
				return await ReadStructColumnAsync<double>(groupReader, field, rows, cancellationToken);
			else if (type == typeof(decimal))
				return await ReadStructColumnAsync<decimal>(groupReader, field, rows, cancellationToken);
			else if (type == typeof(DateTime))
				return await ReadStructColumnAsync<DateTime>(groupReader, field, rows, cancellationToken);
			else if (type == typeof(DateOnly))
				return await ReadStructColumnAsync<DateOnly>(groupReader, field, rows, cancellationToken);
			else if (type == typeof(TimeOnly))
				return await ReadStructColumnAsync<TimeOnly>(groupReader, field, rows, cancellationToken);
			else if (type == typeof(TimeSpan))
				return await ReadStructColumnAsync<TimeSpan>(groupReader, field, rows, cancellationToken);
			else if (type == typeof(Guid))
				return await ReadStructColumnAsync<Guid>(groupReader, field, rows, cancellationToken);
			else
				throw new NotSupportedException($"Can't read the column '{field.Name}' of type '{type.Name}'");
	}

	/// <summary>
	///		Lee una columna de un tipo valor (admita o no nulos) y devuelve sus valores como <see cref="object"/>
	/// </summary>
	private static async Task<object?[]> ReadStructColumnAsync<T>(ParquetRowGroupReader groupReader, DataField field, int rows, CancellationToken cancellationToken) where T : struct
	{
		object?[] result = new object?[rows];

			// Lee los valores (con una sobrecarga distinta según si la columna admite nulos)
			if (field.IsNullable)
			{
				T?[] values = new T?[rows];

					await groupReader.ReadAsync<T>(field, values.AsMemory(), cancellationToken: cancellationToken);
					for (int index = 0; index < rows; index++)
						result[index] = values[index];
			}
			else
			{
				T[] values = new T[rows];

					await groupReader.ReadAsync<T>(field, values.AsMemory(), cancellationToken: cancellationToken);
					for (int index = 0; index < rows; index++)
						result[index] = values[index];
			}
			// Devuelve los valores convertidos
			return result;
	}

	/// <summary>
	///		Lanza el evento de lectura de un bloque
	/// </summary>
	private void RaiseEventReadBlock(long row)
	{
		if (NotifyAfter > 0 && row % NotifyAfter == 0)
			Progress?.Invoke(this, new EventArguments.AffectedEvntArgs(row));
	}

	/// <summary>
	///		Cierra el archivo
	/// </summary>
	public void Close()
	{
		// Cierra el lector de parquet
		if (_parquetReader != null)
		{
			ParquetReader reader = _parquetReader;

				_parquetReader = null;
				Task.Run(async () => await reader.DisposeAsync()).GetAwaiter().GetResult();
		}
		// Cierra el stream del archivo
		if (_fileReader != null)
		{
			_fileReader.Close();
			_fileReader = null;
		}
		// Indica que está cerrado
		IsClosed = true;
	}

	/// <summary>
	///		Obtiene el nombre del campo
	/// </summary>
	public string GetName(int i)
	{
		EnsureSchema();
		return _schema[i].Name;
	}

	/// <summary>
	///		Obtiene el nombre del tipo de datos
	/// </summary>
	public string GetDataTypeName(int i) => GetFieldType(i).Name;

	/// <summary>
	///		Obtiene el tipo de un campo
	/// </summary>
	public Type GetFieldType(int i)
	{
		EnsureSchema();
		return _schema[i].ClrType;
	}

	/// <summary>
	///		Obtiene el valor de un campo (<see cref="DBNull.Value"/> si es nulo)
	/// </summary>
	public object GetValue(int i)
	{
		object? value = EnsureCurrentRow()[i];

			return value ?? DBNull.Value;
	}

	/// <summary>
	///		Obtiene el valor de un campo distinto de <see cref="DBNull"/> o lanza si lo es
	/// </summary>
	private object GetNonDbNullValue(int i)
	{
		object value = GetValue(i);

			if (value is DBNull)
				throw new InvalidCastException($"The value of column '{GetName(i)}' is null");
			return value;
	}

	public System.Data.DataTable GetSchemaTable()
	{
		EnsureSchema();

		System.Data.DataTable table = new("SchemaTable");

			table.Columns.Add("ColumnName", typeof(string));
			table.Columns.Add("ColumnOrdinal", typeof(int));
			table.Columns.Add("ColumnSize", typeof(int));
			table.Columns.Add("DataType", typeof(Type));
			table.Columns.Add("AllowDBNull", typeof(bool));
			for (int index = 0; index < _schema.Length; index++)
			{
				System.Data.DataRow row = table.NewRow();

					row["ColumnName"] = _schema[index].Name;
					row["ColumnOrdinal"] = index;
					row["ColumnSize"] = -1;
					row["DataType"] = _schema[index].ClrType;
					row["AllowDBNull"] = _schema[index].IsNullable;
					table.Rows.Add(row);
			}
			return table;
	}

	public int GetValues(object[] values)
	{
		int count = Math.Min(FieldCount, values.Length);

			for (int index = 0; index < count; index++)
				values[index] = GetValue(index);
			return count;
	}

	public bool GetBoolean(int i) => Convert.ToBoolean(GetNonDbNullValue(i), CultureInfo.InvariantCulture);

	public byte GetByte(int i) => Convert.ToByte(GetNonDbNullValue(i), CultureInfo.InvariantCulture);

	public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
	{
		object value = GetNonDbNullValue(i);
		byte[] source = value as byte[] ?? throw new InvalidCastException($"The value of column '{GetName(i)}' ({value.GetType().Name}) is not a byte array");

			if (buffer is null)
				return source.Length;

			int count = (int) Math.Min(length, source.Length - fieldOffset);

				Array.Copy(source, fieldOffset, buffer, bufferoffset, count);
				return count;
	}

	public char GetChar(int i)
	{
		string text = GetString(i);

			if (text.Length == 1)
				return text[0];
			else
				throw new InvalidCastException($"The value of column '{GetName(i)}' is not a single character");
	}

	public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
	{
		object value = GetNonDbNullValue(i);
		string source = value as string ?? throw new InvalidCastException($"The value of column '{GetName(i)}' ({value.GetType().Name}) is not a string");

			if (buffer is null)
				return source.Length;

			int count = (int) Math.Min(length, source.Length - fieldoffset);

				source.CopyTo((int) fieldoffset, buffer, bufferoffset, count);
				return count;
	}

	public Guid GetGuid(int i)
	{
		object value = GetNonDbNullValue(i);

			if (value is Guid guid)
				return guid;
			else
				throw new InvalidCastException($"The value of column '{GetName(i)}' ({value.GetType().Name}) is not a Guid");
	}

	public short GetInt16(int i) => Convert.ToInt16(GetNonDbNullValue(i), CultureInfo.InvariantCulture);

	public int GetInt32(int i) => Convert.ToInt32(GetNonDbNullValue(i), CultureInfo.InvariantCulture);

	public long GetInt64(int i) => Convert.ToInt64(GetNonDbNullValue(i), CultureInfo.InvariantCulture);

	public float GetFloat(int i) => Convert.ToSingle(GetNonDbNullValue(i), CultureInfo.InvariantCulture);

	public double GetDouble(int i) => Convert.ToDouble(GetNonDbNullValue(i), CultureInfo.InvariantCulture);

	public string GetString(int i)
	{
		object value = GetNonDbNullValue(i);

			if (value is string text)
				return text;
			else
				throw new InvalidCastException($"The value of column '{GetName(i)}' ({value.GetType().Name}) is not a string");
	}

	public decimal GetDecimal(int i) => Convert.ToDecimal(GetNonDbNullValue(i), CultureInfo.InvariantCulture);

	public DateTime GetDateTime(int i)
	{
		object value = GetNonDbNullValue(i);

			return value switch
				{
					DateTime dateTime => dateTime,
					DateOnly dateOnly => dateOnly.ToDateTime(TimeOnly.MinValue),
					_ => Convert.ToDateTime(value, CultureInfo.InvariantCulture)
				};
	}

	public System.Data.IDataReader GetData(int i)
	{
		throw new NotSupportedException("Parquet files don't support nested resultsets");
	}

	/// <summary>
	///		Obtiene el índice de un campo a partir de su nombre
	/// </summary>
	public int GetOrdinal(string name)
	{
		// Obtiene el índice del registro
		if (!string.IsNullOrWhiteSpace(name))
		{
			// Se asegura de que el esquema esté disponible
			EnsureSchema();
			// Busca el campo por nombre (sin distinguir mayúsculas / minúsculas)
			for (int index = 0; index < _schema.Length; index++)
				if (_schema[index].Name.Equals(name, StringComparison.CurrentCultureIgnoreCase))
					return index;
		}
		// Si ha llegado hasta aquí es porque no ha encontrado el campo
		return -1;
	}

	/// <summary>
	///		Indica si el campo es un DbNull
	/// </summary>
	public bool IsDBNull(int i)
	{
		object? value = EnsureCurrentRow()[i];

			return value is null || value is DBNull;
	}

	/// <summary>
	///		Los CSV sólo devuelven un Resultset, de todas formas, DbDataAdapter espera este valor
	/// </summary>
	public bool NextResult() => false;

	/// <summary>
	///		Libera la memoria
	/// </summary>
	protected virtual void Dispose(bool disposing)
	{
		if (!Disposed)
		{
			// Libera los datos
			if (disposing)
				Close();
			// Indica que se ha liberado
			Disposed = true;
		}
	}

	/// <summary>
	///		Libera la memoria
	/// </summary>
	public void Dispose()
	{
		Dispose(true);
		GC.SuppressFinalize(this);
	}

	/// <summary>
	///		Profundidad del recordset
	/// </summary>
	public int Depth => 0;

	/// <summary>
	///		Indica si está cerrado
	/// </summary>
	public bool IsClosed { get; private set; } = true;

	/// <summary>
	///		Registros afectados
	/// </summary>
	public int RecordsAffected  => -1;

	/// <summary>
	///		Bloque de filas para las que se lanza el evento de grabación
	/// </summary>
	public int NotifyAfter { get; }

	/// <summary>
	///		Número de campos a partir de las columnas
	/// </summary>
	/// <remarks>
	///		Lo primero que hace un BulkCopy es ver el número de campos que tiene, si no se ha leido la cabecera puede
	///	que aún no tengamos ningún número de columnas, por eso se lee por primera vez
	/// </remarks>
	public int FieldCount
	{
		get
		{
			// Lee la cabecera para cargar las columnas si es necesario
			if (_schema is null || _schema.Length == 0)
				ParseSchema();
			// Devuelve el número de columnas
			return _schema?.Length ?? 0;
		}
	}

	/// <summary>
	///		Indexador por número de campo
	/// </summary>
	public object this[int i] => GetValue(i);

	/// <summary>
	///		Indexador por nombre de campo
	/// </summary>
	public object this[string name] => GetValue(GetOrdinal(name));

	/// <summary>
	///		Indica si se ha liberado el recurso
	/// </summary>
	public bool Disposed { get; private set; }
}