using System;
using System.Collections.Generic;
using DbData = System.Data;
using System.Linq;

using Parquet;
using Parquet.Data;

namespace Bau.Libraries.LibParquetFiles.Readers
{
	/// <summary>
	///		Implementación de <see cref="DbData.IDataReader"/> para archivos Parquet
	/// </summary>
	public class ParquetDataReader : DbData.IDataReader
	{
		// Eventos públicos
		public event EventHandler<EventArguments.AffectedEvntArgs> Progress;
		// Variables privadas
		private System.IO.Stream _fileReader;
		private ParquetReader _parquetReader;
		private DataField[] _schema = null;
		private DataColumn[] _groupRowColumns = null;
		private int _rowGroup = 0, _actualRow = 0;
		private List<object> _rowValues;
		private long _row;

		public ParquetDataReader(string fileName, int notifyAfter = 10_000)
		{
			FileName = fileName;
			NotifyAfter = notifyAfter;
		}

		/// <summary>
		///		Abre el archivo
		/// </summary>
		public void Open()
		{
			if (IsClosed)
			{
				// Abre el stream
				_fileReader = System.IO.File.OpenRead(FileName);
				_parquetReader = new ParquetReader(_fileReader);
				// e indica que aún no se ha leido ninguna línea
				_row = 0;
				_rowGroup = 0;
				_actualRow = 0;
				// Indica que está abierto
				IsClosed = false;
			}
		}

		/// <summary>
		///		Lee un registro
		/// </summary>
		public bool Read()
		{
			bool readed = false;

				// Recorre los grupos de filas del archivo
				if (_groupRowColumns == null || _actualRow >= _groupRowColumns[0].Data.Length)
				{
					// Obtiene el lector con el grupo de filas
					if (_rowGroup < _parquetReader.RowGroupCount)
						_groupRowColumns = _parquetReader.ReadEntireRowGroup(_rowGroup).ToArray();
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
					_rowValues = new List<object>();
					foreach (DataColumn column in _groupRowColumns)
					{
						object value = column.Data.GetValue(_actualRow);

							// Parquet almacena las fechas como DateTimeOffset y se debe convertir a un dateTime
							if (value is DateTimeOffset date)
								value = ConvertFromDateTimeOffset(date);
							// Añade el valor 
							_rowValues.Add(value);
					}
					// Indica que se ha leido el registro e incrementa la fila actual
					readed = true;
					_actualRow++;
					// Incrementa la fila total y lanza el evento
					_row++;
					if (_row % NotifyAfter == 0)
						RaiseEventReadBlock(_row);
				}
				// Devuelve el valor que indica si se ha leido un registro
				return readed;
		}

		/// <summary>
		///		Convierte correctamente un <see cref="DateTimeOffset"/> en <see cref="DateTime"/>
		/// </summary>
		private DateTime ConvertFromDateTimeOffset(DateTimeOffset dateTime)
		{
		   if (dateTime.Offset.Equals(TimeSpan.Zero))
			  return dateTime.UtcDateTime;
		   else if (dateTime.Offset.Equals(TimeZoneInfo.Local.GetUtcOffset(dateTime.DateTime)))
			  return DateTime.SpecifyKind(dateTime.DateTime, DateTimeKind.Local);
		   else
			  return dateTime.DateTime;   
		}

		/// <summary>
		///		Interpreta el esquema del archivos
		/// </summary>
		private void ParseSchema()
		{
			// Abre el archivo
			Open();
			// Lee los campos del esquema
			_schema = _parquetReader.Schema.GetDataFields();
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
				_parquetReader.Dispose();
				_parquetReader = null;
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
			return _schema[i].Name;
		}

		/// <summary>
		///		Obtiene el nombre del tipo de datos
		/// </summary>
		public string GetDataTypeName(int i)
		{
			return _rowValues[i].GetType().Name;
		}

		/// <summary>
		///		Obtiene el tipo de un campo
		/// </summary>
		public Type GetFieldType(int i)
		{
			return _rowValues[i].GetType();
		}

		/// <summary>
		///		Obtiene el valor de un campo
		/// </summary>
		public object GetValue(int i)
		{
			return _rowValues[i];
		}

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
				for (int index = 0; index < _schema.Length; index++)
					if (_schema[index].Name.Equals(name, StringComparison.CurrentCultureIgnoreCase))
						return index;
			// Si ha llegado hasta aquí es porque no ha encontrado el campo
			return -1;
		}

		/// <summary>
		///		Indica si el campo es un DbNull
		/// </summary>
		public bool IsDBNull(int i)
		{
			return _rowValues[i] == null || _rowValues[i] is DBNull;
		}

		/// <summary>
		///		Los CSV sólo devuelven un Resultset, de todas formas, DbDataAdapter espera este valor
		/// </summary>
		public bool NextResult()
		{
			return false;
		}

		/// <summary>
		///		Libera la memoria
		/// </summary>
		protected virtual void Dispose(bool disposing)
		{
			if (!IsDisposed)
			{
				// Libera los datos
				if (disposing)
					Close();
				// Indica que se ha liberado
				IsDisposed = true;
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
		public int Depth 
		{ 
			get { return 0; }
		}

		/// <summary>
		///		Indica si está cerrado
		/// </summary>
		public bool IsClosed { get; private set; } = true;

		/// <summary>
		///		Registros afectados
		/// </summary>
		public int RecordsAffected 
		{ 
			get { return -1; }
		}

		/// <summary>
		///		Nombre del archivo
		/// </summary>
		public string FileName { get; }

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
				if (_schema == null || _schema.Length == 0)
					ParseSchema();
				// Devuelve el número de columnas
				return _schema.Length; 
			}
		}

		/// <summary>
		///		Indizador por número de campo
		/// </summary>
		public object this[int i] 
		{ 
			get { return _rowValues[i]; }
		}

		/// <summary>
		///		Indizador por nombre de campo
		/// </summary>
		public object this[string name]
		{ 
			get { return _rowValues[GetOrdinal(name)]; }
		}

		/// <summary>
		///		Indica si se ha liberado el recurso
		/// </summary>
		public bool IsDisposed { get; private set; }
	}
}