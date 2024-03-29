﻿using DbData = System.Data; // ... para que no colisione con el objeto DataColumn de Parquet.Data

using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace Bau.Libraries.LibParquetFiles.Readers;

/// <summary>
///		Implementación de <see cref="DbData.IDataReader"/> para archivos Parquet
/// </summary>
public class ParquetDataReader : DbData.IDataReader
{
	// Eventos públicos
	public event EventHandler<EventArguments.AffectedEvntArgs>? Progress;
	// Variables privadas
	private Stream? _fileReader;
	private ParquetReader? _parquetReader;
	private DataField[]? _schema;
	private DataColumn[]? _groupRowColumns;
	private int _rowGroup = 0, _actualRow = 0;
	private List<object?> _rowValues = default!;
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
		// Asigna el stream al archivo
		_fileReader = stream;
		_parquetReader = await ParquetReader.CreateAsync(_fileReader, cancellationToken: cancellationToken);
		// e indica que aún no se ha leido ninguna línea
		_row = 0;
		_rowGroup = 0;
		_actualRow = 0;
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
			if (_parquetReader is not null)
			{
				// Recorre los grupos de filas del archivo
				if (_groupRowColumns == null || _actualRow >= _groupRowColumns[0].Data.Length)
				{
					// Obtiene el lector con el grupo de filas
					if (_rowGroup < _parquetReader.RowGroupCount)
						_groupRowColumns = (await _parquetReader.ReadEntireRowGroupAsync(_rowGroup)).ToArray();
					else
						_groupRowColumns = null;
					// Incrementa el número de grupo y cambia la fila actual
					_rowGroup++;
					_actualRow = 0;
				}
				// Obtiene los datos (si queda algo por leer)
				if (_groupRowColumns != null)
				{
					// Transforma las columnas
					_rowValues = new List<object?>();
					foreach (DataColumn column in _groupRowColumns)
						_rowValues.Add(column.Data.GetValue(_actualRow));
//TODO ¿esto sigue haciendo falta? Posiblemente aumente mucho el rendimiento si quitamos todo este If
/*
					{
						object? value = column.Data.GetValue(_actualRow);

							// Cambia el tipo GUID
							if (value is not null)
							{
								if (IsGuid(value))
									value = ConvertGuidFromString(value);
							}
							// Añade el valor 
							_rowValues.Add(value);
					}
*/

					// Indica que se ha leido el registro e incrementa la fila actual
					readed = true;
					_actualRow++;
					// Incrementa la fila total y lanza el evento
					_row++;
					if (_row % NotifyAfter == 0)
						RaiseEventReadBlock(_row);
				}
			}
			// Devuelve el valor que indica si se ha leido un registro
			return readed;
	}

	/// <summary>
	///		Comprueba si un objeto es un GUID
	/// </summary>
	private bool IsGuid(object value)
	{
		if (value != null)
			return Guid.TryParse(value.ToString(), out Guid _);
		else
			return false;
	}

	/// <summary>
	///		Convierte un objeto a un GUID
	/// </summary>
	private Guid? ConvertGuidFromString(object value)
	{
		if (value != null && Guid.TryParse(value.ToString(), out Guid result))
			return result;
		else
			return null;
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
		if (_parquetReader is not null)
		{
			_parquetReader.Dispose();
			_parquetReader = null;
		}
		// Cierra el stream del archivo
		if (_fileReader is not null)
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
	public string GetName(int i) => Schema[i].Name;

	/// <summary>
	///		Obtiene el nombre del tipo de datos
	/// </summary>
	public string GetDataTypeName(int i) => GetFieldType(i).Name;

	/// <summary>
	///		Obtiene el tipo de un campo
	/// </summary>
	public Type GetFieldType(int i) => Schema[i].ClrType; // _rowValues[i].GetType();

	/// <summary>
	///		Obtiene el valor de un campo
	/// </summary>
	public object GetValue(int i) => _rowValues[i];

	public DbData.DataTable GetSchemaTable()
	{
		throw new NotImplementedException();
	}

	public int GetValues(object[] values)
	{
		throw new NotImplementedException();
	}

	public bool GetBoolean(int i)
	{
		throw new NotImplementedException();
	}

	public byte GetByte(int i)
	{
		throw new NotImplementedException();
	}

	public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
	{
		throw new NotImplementedException();
	}

	public char GetChar(int i)
	{
		throw new NotImplementedException();
	}

	public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
	{
		throw new NotImplementedException();
	}

	public Guid GetGuid(int i)
	{
		throw new NotImplementedException();
	}

	public short GetInt16(int i)
	{
		throw new NotImplementedException();
	}

	public int GetInt32(int i)
	{
		throw new NotImplementedException();
	}

	public long GetInt64(int i)
	{
		throw new NotImplementedException();
	}

	public float GetFloat(int i)
	{
		throw new NotImplementedException();
	}

	public double GetDouble(int i)
	{
		throw new NotImplementedException();
	}

	public string GetString(int i)
	{
		throw new NotImplementedException();
	}

	public decimal GetDecimal(int i)
	{
		throw new NotImplementedException();
	}

	public DateTime GetDateTime(int i)
	{
		throw new NotImplementedException();
	}

	public DbData.IDataReader GetData(int i)
	{
		throw new NotImplementedException();
	}

	/// <summary>
	///		Obtiene el índice de un campo a partir de su nombre
	/// </summary>
	public int GetOrdinal(string name)
	{
		// Obtiene el índice del registro
		if (!string.IsNullOrWhiteSpace(name))
			for (int index = 0; index < Schema.Length; index++)
				if (Schema[index].Name.Equals(name, StringComparison.CurrentCultureIgnoreCase))
					return index;
		// Si ha llegado hasta aquí es porque no ha encontrado el campo
		return -1;
	}

	/// <summary>
	///		Indica si el campo es un DbNull
	/// </summary>
	public bool IsDBNull(int i) => _rowValues[i] is null || _rowValues[i] is DBNull;

	/// <summary>
	///		Los CSV sólo devuelven un Resultset, de todas formas, DbDataAdapter espera este valor
	/// </summary>
	public bool NextResult() => false;

	/// <summary>
	///		Esquema del archivo
	/// </summary>
	private DataField[]? Schema
	{
		get 
		{
			// Si no se ha leído aún el esquema, se lee
			if (_schema is null && _parquetReader is not null)
				_schema = _parquetReader.Schema.GetDataFields();
			// Devuelve el esquema
			return _schema;
		}
	}

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
	public int FieldCount => Schema?.Length ?? 0;

	/// <summary>
	///		Indexador por número de campo
	/// </summary>
	public object this[int i] => _rowValues[i];

	/// <summary>
	///		Indexador por nombre de campo
	/// </summary>
	public object this[string name] => _rowValues[GetOrdinal(name)];

	/// <summary>
	///		Indica si se ha liberado el recurso
	/// </summary>
	public bool Disposed { get; private set; }
}