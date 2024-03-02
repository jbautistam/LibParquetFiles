using System.Data;
using System.Diagnostics.CodeAnalysis;

namespace ParquetFiles.Tests.MotherObject;

/// <summary>
///		<see cref="IDataReader"/> sobre <see cref="Sale"/>
/// </summary>
internal class SalesDataReader : IDataReader
{
	// Variables privadas
	private List<string> _fields = new() {
											"Id", 
											"ProductId",
											"Date",
											"Price",
											"Quantity"
										};
	private bool _startRead = false;

	internal SalesDataReader(List<Sale> sales)
	{
		Sales = sales;
		ActualRow = 0;
	}

	public void Close()
	{
	}

	public DataTable? GetSchemaTable()
	{
	throw new NotImplementedException();
	}

	public bool NextResult()
	{
		throw new NotImplementedException();
	}

	public bool Read()
	{
			if (_startRead)
				ActualRow++;
			else
			{
				_startRead = true;
				ActualRow = 0;
			}
			return ActualRow < Sales.Count;
	}

	public int Depth { get; } = 1;

	public bool IsClosed { get; }

	public int RecordsAffected { get; }

	public bool GetBoolean(int i)
	{
	throw new NotImplementedException();
	}

	public byte GetByte(int i)
	{
	throw new NotImplementedException();
	}

	public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
	{
	throw new NotImplementedException();
	}

	public char GetChar(int i)
	{
	throw new NotImplementedException();
	}

	public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
	{
	throw new NotImplementedException();
	}

	public IDataReader GetData(int i)
	{
	throw new NotImplementedException();
	}

	public string GetDataTypeName(int i)
	{
	throw new NotImplementedException();
	}

	public DateTime GetDateTime(int i)
	{
	throw new NotImplementedException();
	}

	public decimal GetDecimal(int i)
	{
	throw new NotImplementedException();
	}

	public double GetDouble(int i) 
	{
		if (GetValue(i) is double value) 
			return value;
		else
			return -1;
	}

	[return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
	public Type GetFieldType(int i) => GetValue(i).GetType();

	public float GetFloat(int i)
	{
	throw new NotImplementedException();
	}

	public Guid GetGuid(int i) 
	{
		if (GetValue(i) is Guid guid)
			return guid;
		else
			return Guid.NewGuid();
	}

	public short GetInt16(int i)
	{
	throw new NotImplementedException();
	}

	public int GetInt32(int i)
	{
		if (GetValue(i) is int value) 
			return value;
		else
			return -1;
	}

	public long GetInt64(int i)
	{
	throw new NotImplementedException();
	}

	/// <summary>
	///		Obtiene el nombre a partir del ordinal
	/// </summary>
	public string GetName(int i) => _fields[i];

	/// <summary>
	///		Obtiene el ordinal de un campo
	/// </summary>
	public int GetOrdinal(string name)
	{
		// Busca el nombre del campo
		for (int index = 0; index < _fields.Count; index++)
			if (_fields[index].Equals(name, StringComparison.CurrentCultureIgnoreCase))
				return index;
		// Si ha llegado hasta aquí es porque no ha encontrado nada
		return -1;
	}

	public string GetString(int i) => GetValue(i)?.ToString();

	/// <summary>
	///		Obtiene un valor por el índice
	/// </summary>
	public object GetValue(int i)
	{
		if (ActualRow == Sales.Count)
			System.Diagnostics.Debug.WriteLine("Stop");
		switch (i)
		{
			case 0:
				return Sales[ActualRow].Id;
			case 1:
				return Sales[ActualRow].ProductId;
			case 2:
				return Sales[ActualRow].Date;
			case 3:
				return Sales[ActualRow].Price;
			case 4:
				return Sales[ActualRow].Quantity;
			default:
				throw new ArgumentException();
		}
	}

	public int GetValues(object[] values)
	{
	throw new NotImplementedException();
	}

	/// <summary>
	///		Comprueba si un valor es nulo
	/// </summary>
	public bool IsDBNull(int i) => GetValue(i) is null || GetValue(i) is DBNull;

	/// <summary>
	///		Número de campos
	/// </summary>
	public int FieldCount => _fields.Count;

	/// <summary>
	///		Obtiene el valor por índice
	/// </summary>
	public object this[int i] => GetValue(i);

	/// <summary>
	///		Obtiene el valor por nombre
	/// </summary>
	public object this[string name] => GetValue(GetOrdinal(name));

	/// <summary>
	///		Libera la memoria
	/// </summary>
	protected virtual void Dispose(bool disposing)
	{
		IsDisposed = true;
	}

	/// <summary>
	///		Libera la memoria
	/// </summary>
	public void Dispose()
	{
		Dispose(disposing: true);
		GC.SuppressFinalize(this);
	}

	/// <summary>
	///		Indica si se ha liberado la memoria
	/// </summary>
	private bool IsDisposed { get; set; }

	/// <summary>
	///		Ventas
	/// </summary>
	public List<Sale> Sales { get; }

	/// <summary>
	///		Fila actual
	/// </summary>
	public int ActualRow { get; private set; }
}
