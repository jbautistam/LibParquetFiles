using System.Data;

namespace ParquetFiles.Tests.MotherObject;

/// <summary>
///		<see cref="IDataReader"/> en memoria construido a partir de un esquema de columnas y una lista de filas
/// </summary>
/// <remarks>
///		A diferencia de <see cref="SalesDataReader"/> (pensado sólo para el escenario de la prueba original), este
///	lector admite cualquier tipo de columna, valores nulos, 0 columnas, 0 filas y nombres vacíos o duplicados. Los
///	getters tipados tienen semántica estricta de ADO.NET: si el valor no es exactamente del tipo pedido (o es nulo)
///	lanzan, en lugar de devolver un valor de relleno que enmascare un fallo del código bajo prueba
/// </remarks>
internal class FakeDataReader : IDataReader
{
	private readonly IReadOnlyList<object?[]> _rows;
	private int _actualRow = -1;

	internal FakeDataReader(IReadOnlyList<(string Name, Type Type)> columns, IReadOnlyList<object?[]> rows)
	{
		Columns = columns;
		_rows = rows;
	}

	/// <summary>
	///		Crea un lector de una única columna a partir de sus valores (para la matriz de tipos)
	/// </summary>
	internal static FakeDataReader CreateSingleColumn<T>(string name, IEnumerable<T?> values)
	{
		return CreateSingleColumn(name, typeof(T), values.Cast<object?>().ToList());
	}

	/// <summary>
	///		Crea un lector de una única columna indicando el tipo CLR explícitamente (para pruebas parametrizadas
	///	donde el tipo de columna varía en tiempo de ejecución)
	/// </summary>
	internal static FakeDataReader CreateSingleColumn(string name, Type type, IReadOnlyList<object?> values)
	{
		List<object?[]> rows = new();

			foreach (object? value in values)
				rows.Add(new object?[] { value is null ? DBNull.Value : value });
			return new FakeDataReader(new[] { (name, type) }, rows);
	}

	public void Close()
	{
		IsClosed = true;
	}

	public void Dispose()
	{
		Close();
	}

	public DataTable GetSchemaTable() => throw new NotImplementedException();

	public bool NextResult() => false;

	public bool Read()
	{
		if (_actualRow + 1 < _rows.Count)
		{
			_actualRow++;
			return true;
		}
		return false;
	}

	public int Depth => 0;

	public bool IsClosed { get; private set; }

	public int RecordsAffected => -1;

	/// <summary>
	///		Obtiene el valor bruto de una columna de la fila actual (puede ser <see cref="DBNull"/>)
	/// </summary>
	public object GetValue(int i)
	{
		if (_actualRow < 0 || _actualRow >= _rows.Count)
			throw new InvalidOperationException("No hay ninguna fila actual: llame antes a Read()");
		return _rows[_actualRow][i] ?? DBNull.Value;
	}

	public int GetValues(object[] values)
	{
		int count = Math.Min(FieldCount, values.Length);

			for (int index = 0; index < count; index++)
				values[index] = GetValue(index);
			return count;
	}

	public bool IsDBNull(int i) => GetValue(i) is DBNull;

	private T GetNonNull<T>(int i)
	{
		object value = GetValue(i);

			if (value is DBNull)
				throw new InvalidCastException($"El valor de la columna '{GetName(i)}' es nulo");
			else if (value is T typed)
				return typed;
			else
				throw new InvalidCastException($"No se puede convertir el valor '{value}' ({value.GetType().Name}) de la columna '{GetName(i)}' a {typeof(T).Name}");
	}

	public bool GetBoolean(int i) => GetNonNull<bool>(i);

	public byte GetByte(int i) => GetNonNull<byte>(i);

	public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
	{
		byte[] source = GetNonNull<byte[]>(i);

			if (buffer is null)
				return source.Length;

			int count = Math.Min(length, source.Length - (int) fieldOffset);

				Array.Copy(source, fieldOffset, buffer, bufferoffset, count);
				return count;
	}

	public char GetChar(int i) => GetNonNull<char>(i);

	public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
	{
		string source = GetNonNull<string>(i);

			if (buffer is null)
				return source.Length;

			int count = Math.Min(length, source.Length - (int) fieldoffset);

				source.CopyTo((int) fieldoffset, buffer, bufferoffset, count);
				return count;
	}

	public IDataReader GetData(int i) => throw new NotSupportedException();

	public string GetDataTypeName(int i) => GetFieldType(i).Name;

	public DateTime GetDateTime(int i) => GetNonNull<DateTime>(i);

	public decimal GetDecimal(int i) => GetNonNull<decimal>(i);

	public double GetDouble(int i) => GetNonNull<double>(i);

	public Type GetFieldType(int i) => Columns[i].Type;

	public float GetFloat(int i) => GetNonNull<float>(i);

	public Guid GetGuid(int i) => GetNonNull<Guid>(i);

	public short GetInt16(int i) => GetNonNull<short>(i);

	public int GetInt32(int i) => GetNonNull<int>(i);

	public long GetInt64(int i) => GetNonNull<long>(i);

	public string GetName(int i) => Columns[i].Name;

	public int GetOrdinal(string name)
	{
		for (int index = 0; index < Columns.Count; index++)
			if (Columns[index].Name.Equals(name, StringComparison.CurrentCultureIgnoreCase))
				return index;
		return -1;
	}

	public string GetString(int i) => GetNonNull<string>(i);

	public int FieldCount => Columns.Count;

	public object this[int i] => GetValue(i);

	public object this[string name] => GetValue(GetOrdinal(name));

	/// <summary>
	///		Esquema de columnas (nombre, tipo)
	/// </summary>
	internal IReadOnlyList<(string Name, Type Type)> Columns { get; }
}
