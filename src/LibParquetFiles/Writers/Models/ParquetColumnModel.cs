using Parquet;
using Parquet.Schema;

namespace Bau.Libraries.LibParquetFiles.Writers.Models;

/// <summary>
///		Modelo de escritura de una columna
/// </summary>
public class ParquetColumnModel
{
	/// <summary>
	///		Tipo del campo
	/// </summary>
	public enum FieldType
	{
		/// <summary>Valor lógico</summary>
		Boolean,
		/// <summary>Fecha / hora</summary>
		DateTime,
		/// <summary>Byte</summary>
		Byte,
		/// <summary>Entero</summary>
		Integer,
		/// <summary>Entero largo</summary>
		Long,
		/// <summary>Decimal</summary>
		Decimal,
		/// <summary>doble</summary>
		Double,
		/// <summary>Cadena</summary>
		String,
		/// <summary>Guid: en las grabaciones se tratará como cadena</summary>
		Guid,
		/// <summary>Hora / duración</summary>
		Time,
		/// <summary>Matriz de bytes</summary>
		ByteArray
	}
	// Variables privadas
	private readonly int _maxValues;
	private string?[] _stringValues = default!;
	private int?[] _intValues = default!;
	private DateTime?[] _dateTimeValues = default!;
	private long?[] _longValues = default!;
	private double?[] _doubleValues = default!;
	private decimal?[] _decimalValues = default!;
	private bool?[] _boolValues = default!;
	private byte?[] _byteValues = default!;
	private Guid?[] _guidValues = default!;
	private TimeSpan?[] _timeValues = default!;
	private byte[]?[] _byteArrayValues = default!;

	public ParquetColumnModel(FieldType fieldType, string name, int maxValues)
	{
		ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxValues);
		// Asigna las propiedades
		Type = fieldType;
		Name = name;
		_maxValues = maxValues;
		// Crea el campo parquet
		ParquetField = ConvertType(fieldType, name);
		// Crea los arrays de valores
		switch (fieldType)
		{
			case FieldType.Boolean:
					_boolValues = new bool?[maxValues];
				break;
			case FieldType.Byte:
					_byteValues = new byte?[maxValues];
				break;
			case FieldType.DateTime:
					_dateTimeValues = new DateTime?[maxValues];
				break;
			case FieldType.Decimal:
					_decimalValues = new decimal?[maxValues];
				break;
			case FieldType.Double:
					_doubleValues = new double?[maxValues];
				break;
			case FieldType.Integer:
					_intValues = new int?[maxValues];
				break;
			case FieldType.Long:
					_longValues = new long?[maxValues];
				break;
			case FieldType.Guid:
					_guidValues = new Guid?[maxValues];
				break;
			case FieldType.Time:
					_timeValues = new TimeSpan?[maxValues];
				break;
			case FieldType.ByteArray:
					_byteArrayValues = new byte[]?[maxValues];
				break;
			default:
					_stringValues = new string[maxValues];
				break;
		}
	}

	/// <summary>
	///		Comprueba que aún queda espacio en el buffer antes de añadir un valor más
	/// </summary>
	private void CheckCapacity()
	{
		if (Count >= _maxValues)
			throw new InvalidOperationException($"No se pueden añadir más valores a la columna '{Name}': se ha alcanzado el tamaño máximo del grupo de filas ({_maxValues})");
	}

	/// <summary>
	///		Añade un nulo
	/// </summary>
	public void AddNull()
	{
		CheckCapacity();
		// Asigna el valor
		switch (Type)
		{
			case FieldType.Boolean:
					_boolValues[Count] = null;
				break;
			case FieldType.Byte:
					_byteValues[Count] = null;
				break;
			case FieldType.DateTime:
					_dateTimeValues[Count] = null;
				break;
			case FieldType.Decimal:
					_decimalValues[Count] = null;
				break;
			case FieldType.Double:
					_doubleValues[Count] = null;
				break;
			case FieldType.Integer:
					_intValues[Count] = null;
				break;
			case FieldType.Long:
					_longValues[Count] = null;
				break;
			case FieldType.Guid:
					_guidValues[Count] = null;
				break;
			case FieldType.Time:
					_timeValues[Count] = null;
				break;
			case FieldType.ByteArray:
					_byteArrayValues[Count] = null;
				break;
			default:
					_stringValues[Count] = null;
				break;
		}
		// Incrementa el número de registros
		Count++;
	}

	/// <summary>
	///		Escribe los valores de la columna en el <see cref="ParquetRowGroupWriter"/>
	/// </summary>
	public async Task WriteToAsync(ParquetRowGroupWriter writer, CancellationToken cancellationToken)
	{
		switch (Type)
		{
			case FieldType.Boolean:
					await writer.WriteAsync<bool>(ParquetField, _boolValues.AsMemory(0, Count), cancellationToken: cancellationToken);
				break;
			case FieldType.Byte:
					await writer.WriteAsync<byte>(ParquetField, _byteValues.AsMemory(0, Count), cancellationToken: cancellationToken);
				break;
			case FieldType.DateTime:
					await writer.WriteAsync<DateTime>(ParquetField, _dateTimeValues.AsMemory(0, Count), cancellationToken: cancellationToken);
				break;
			case FieldType.Decimal:
					await writer.WriteAsync<decimal>(ParquetField, _decimalValues.AsMemory(0, Count), cancellationToken: cancellationToken);
				break;
			case FieldType.Double:
					await writer.WriteAsync<double>(ParquetField, _doubleValues.AsMemory(0, Count), cancellationToken: cancellationToken);
				break;
			case FieldType.Integer:
					await writer.WriteAsync<int>(ParquetField, _intValues.AsMemory(0, Count), cancellationToken: cancellationToken);
				break;
			case FieldType.Long:
					await writer.WriteAsync<long>(ParquetField, _longValues.AsMemory(0, Count), cancellationToken: cancellationToken);
				break;
			case FieldType.Guid:
					await writer.WriteAsync<Guid>(ParquetField, _guidValues.AsMemory(0, Count), cancellationToken: cancellationToken);
				break;
			case FieldType.Time:
					await writer.WriteAsync<TimeSpan>(ParquetField, _timeValues.AsMemory(0, Count), cancellationToken: cancellationToken);
				break;
			case FieldType.ByteArray:
					// La sobrecarga de Parquet.Net para colecciones (a diferencia de la genérica de arriba) no admite CancellationToken
					await writer.WriteAsync(ParquetField, new ArraySegment<byte[]?>(_byteArrayValues, 0, Count));
				break;
			default:
					// La sobrecarga de Parquet.Net para colecciones (a diferencia de la genérica de arriba) no admite CancellationToken
					await writer.WriteAsync(ParquetField, new ArraySegment<string?>(_stringValues, 0, Count));
				break;
		}
	}

	/// <summary>
	///		Convierte el tipo de datos
	/// </summary>
	private DataField ConvertType(FieldType type, string name)
	{
		return type switch
			{
				FieldType.Boolean => new DataField<bool?>(name),
				FieldType.Decimal => new DataField<decimal?>(name),
				FieldType.Double => new DataField<double?>(name),
				FieldType.Byte => new DataField<byte?>(name),
				FieldType.Integer => new DataField<int?>(name),
				FieldType.Long => new DataField<long?>(name),
				FieldType.DateTime => new DataField<DateTime?>(name),
				FieldType.Guid => new DataField<Guid?>(name),
				FieldType.Time => new DataField<TimeSpan?>(name),
				FieldType.ByteArray => new DataField<byte[]?>(name),
				_ => new DataField<string?>(name)
			};
	}

	/// <summary>
	///		Añade una fecha
	/// </summary>
	public void AddDate(DateTime value)
	{
		CheckCapacity();
		// Asigna el valor
		_dateTimeValues[Count] = value;
		// Incrementa el número de registros
		Count++;
	}

	/// <summary>
	///		Añade un decimal
	/// </summary>
	public void AddDecimal(decimal value)
	{
		CheckCapacity();
		// Asigna el valor
		_decimalValues[Count] = value;
		// Incrementa el número de registros
		Count++;
	}

	/// <summary>
	///		Añade un byte
	/// </summary>
	public void AddByte(byte value)
	{
		CheckCapacity();
		// Asigna el valor
		_byteValues[Count] = value;
		// Incrementa el número de registros
		Count++;
	}

	/// <summary>
	///		Añade un doble
	/// </summary>
	public void AddDouble(double value)
	{
		CheckCapacity();
		// Asigna el valor
		_doubleValues[Count] = value;
		// Incrementa el número de registros
		Count++;
	}

	/// <summary>
	///		Añade un entero
	/// </summary>
	public void AddInteger(int value)
	{
		CheckCapacity();
		// Asigna el valor
		_intValues[Count] = value;
		// Incrementa el número de registros
		Count++;
	}

	/// <summary>
	///		Añade un boolean
	/// </summary>
	public void AddBool(bool value)
	{
		CheckCapacity();
		// Asigna el valor
		_boolValues[Count] = value;
		// Incrementa el número de registros
		Count++;
	}

	/// <summary>
	///		Añade un entero largo
	/// </summary>
	public void AddLong(long value)
	{
		CheckCapacity();
		// Asigna el valor
		_longValues[Count] = value;
		// Incrementa el número de registros
		Count++;
	}

	/// <summary>
	///		Añade una cadena
	/// </summary>
	public void AddString(string? value)
	{
		CheckCapacity();
		// Asigna el valor
		_stringValues[Count] = value;
		// Incrementa el número de registros
		Count++;
	}

	/// <summary>
	///		Añade un Guid
	/// </summary>
	public void AddGuid(Guid value)
	{
		CheckCapacity();
		// Asigna el valor
		_guidValues[Count] = value;
		// Incrementa el número de registros
		Count++;
	}

	/// <summary>
	///		Añade una hora / duración
	/// </summary>
	public void AddTime(TimeSpan value)
	{
		CheckCapacity();
		// Asigna el valor
		_timeValues[Count] = value;
		// Incrementa el número de registros
		Count++;
	}

	/// <summary>
	///		Añade una matriz de bytes
	/// </summary>
	public void AddByteArray(byte[] value)
	{
		CheckCapacity();
		// Asigna el valor
		_byteArrayValues[Count] = value;
		// Incrementa el número de registros
		Count++;
	}

	/// <summary>
	///		Limpia los datos
	/// </summary>
	public void Clear()
	{
		// Limpia las referencias de los buffers que retienen objetos, para no arrastrar memoria del grupo anterior
		if (_stringValues is not null)
			Array.Clear(_stringValues, 0, Count);
		if (_byteArrayValues is not null)
			Array.Clear(_byteArrayValues, 0, Count);
		// Reinicia el contador
		Count = 0;
	}

	/// <summary>
	///		Tipo del campo
	/// </summary>
	public FieldType Type { get; }

	/// <summary>
	///		Nombre del campo
	/// </summary>
	public string Name { get; }

	/// <summary>
	///		Campo de tipo parquet
	/// </summary>
	public DataField ParquetField { get; }

	/// <summary>
	///		Obtiene el número de valores de la columan
	/// </summary>
	public int Count { get; private set; }
}
