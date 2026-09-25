using System.Data;
using System.Globalization;
using Parquet;
using Parquet.Schema;

namespace Bau.Libraries.LibParquetFiles.Writers.Models;

/// <summary>
///		Datos del esquema de un archivo
/// </summary>
public class ParquetFileModel : IAsyncDisposable
{
	public ParquetFileModel(int rowGroupSize)
	{
		RowGroupSize = rowGroupSize;
	}

	/// <summary>
	///		Abre el archivo parquet y lo prepara para escribir los datos
	/// </summary>
	public async Task OpenAsync(Stream stream, IDataReader reader, CancellationToken cancellationToken)
	{
		// Comprueba que el reader tenga columnas que escribir
		if (reader.FieldCount == 0)
			throw new ArgumentException("Can't create a parquet file without columns");
		// Asigna las columnas al esquema
		AssignColumnsSchema(reader);
		// Crea el generador de parquet
		//? No asigna el nivel de compresión: deja el predeterminado para el método
		Writer = await ParquetWriter.CreateAsync(GetParquetSchema(), stream,
													new ParquetOptions { CompressionMethod = CompressionMethod.Snappy },
													cancellationToken: cancellationToken);
	}

	/// <summary>
	///		Asigna las columnas asociadas al <see cref="IDataReader"/> a la caché del archivo
	/// </summary>
	private void AssignColumnsSchema(IDataReader reader)
	{
		// Borra las columnas que hubiera
		Columns.Clear();
		// Obtiene el esquema del dataReader
		for (int index = 0; index < reader.FieldCount; index++)
		{
			if (!string.IsNullOrWhiteSpace(reader.GetName(index)))
				Columns.Add(new ParquetColumnModel(GetColumnSchemaType(reader.GetFieldType(index)), reader.GetName(index), RowGroupSize));
			else
				Columns.Add(new ParquetColumnModel(GetColumnSchemaType(reader.GetFieldType(index)), $"Column{index}", RowGroupSize));
		}
	}

	/// <summary>
	///		Obtiene el esquema Parquet a partir del dataReader
	/// </summary>
	private ParquetSchema GetParquetSchema()
	{
		Field[] fields = new Field[Columns.Count];

			// Obtiene los campos (indica que todos admiten nulos)
			for (int index = 0; index < Columns.Count; index++)
				fields[index] = Columns[index].ParquetField;
			// Devuelve la colección de campos
			return new ParquetSchema(fields);
	}

	/// <summary>
	///		Obtiene el tipo de columna a partir del tipo CLR del campo (desenvolviendo <see cref="Nullable{T}"/> y enumerados)
	/// </summary>
	/// <remarks>
	///		Es <c>internal</c> en lugar de <c>private</c> únicamente para poder probar la tabla de mapeo de tipos
	///	de forma exhaustiva sin tener que pasar por un <see cref="IDataReader"/> completo en cada caso; no amplía
	///	la superficie pública porque toda la clase ya es <c>internal</c>
	/// </remarks>
	public ParquetColumnModel.FieldType GetColumnSchemaType(Type dataType)
	{
		Type type = Nullable.GetUnderlyingType(dataType) ?? dataType;

			// Los enumerados se tratan como su tipo subyacente
			if (type.IsEnum)
				type = Enum.GetUnderlyingType(type);
			// Determina el tipo de columna a partir del tipo CLR exacto (no por coincidencia de nombre)
			if (type == typeof(bool))
				return ParquetColumnModel.FieldType.Boolean;
			else if (type == typeof(byte))
				return ParquetColumnModel.FieldType.Byte;
			else if (type == typeof(sbyte) || type == typeof(short) || type == typeof(ushort) || type == typeof(int))
				return ParquetColumnModel.FieldType.Integer;
			else if (type == typeof(uint) || type == typeof(long))
				return ParquetColumnModel.FieldType.Long;
			else if (type == typeof(ulong) || type == typeof(decimal))
				return ParquetColumnModel.FieldType.Decimal;
			else if (type == typeof(float) || type == typeof(double))
				return ParquetColumnModel.FieldType.Double;
			else if (type == typeof(DateTime) || type == typeof(DateOnly) || type == typeof(DateTimeOffset))
				return ParquetColumnModel.FieldType.DateTime;
			else if (type == typeof(TimeSpan) || type == typeof(TimeOnly))
				return ParquetColumnModel.FieldType.Time;
			else if (type == typeof(Guid))
				return ParquetColumnModel.FieldType.Guid;
			else if (type == typeof(byte[]))
				return ParquetColumnModel.FieldType.ByteArray;
			else
				return ParquetColumnModel.FieldType.String;
	}

	/// <summary>
	///		Lee los datos de un registro y los graba en el archivo
	/// </summary>
	public async Task WriteRecordAsync(IDataReader reader, CancellationToken cancellationToken)
	{
		// Lee los datos del registro
		ReadData(reader);
		// Escribe la caché en el archivo si se ha superado el número máximo de filas
		if (Records >= RowGroupSize)
			await FlushAsync(cancellationToken);
	}

	/// <summary>
	///		Lee los datos del registro
	/// </summary>
	/// <remarks>
	///		Siempre lee con <see cref="IDataReader.GetValue(int)"/> y convierte al tipo de la columna, en lugar de
	///	llamar al getter tipado correspondiente: así el escritor acepta cualquier <see cref="IDataReader"/>, sea
	///	cual sea el subconjunto de getters que implemente
	/// </remarks>
	private void ReadData(IDataReader reader)
	{
		for (int index = 0; index < Columns.Count; index++)
			if (reader.IsDBNull(index))
				Columns[index].AddNull();
			else
			{
				object value = reader.GetValue(index);

					if (value is DBNull)
						Columns[index].AddNull();
					else
						switch (Columns[index].Type)
						{
							case ParquetColumnModel.FieldType.Boolean:
									Columns[index].AddBool(Convert.ToBoolean(value, CultureInfo.InvariantCulture));
								break;
							case ParquetColumnModel.FieldType.Byte:
									Columns[index].AddByte(Convert.ToByte(value, CultureInfo.InvariantCulture));
								break;
							case ParquetColumnModel.FieldType.DateTime:
									Columns[index].AddDate(ConvertToDateTime(value));
								break;
							case ParquetColumnModel.FieldType.Decimal:
									Columns[index].AddDecimal(Convert.ToDecimal(value, CultureInfo.InvariantCulture));
								break;
							case ParquetColumnModel.FieldType.Double:
									Columns[index].AddDouble(Convert.ToDouble(value, CultureInfo.InvariantCulture));
								break;
							case ParquetColumnModel.FieldType.Integer:
									Columns[index].AddInteger(Convert.ToInt32(value, CultureInfo.InvariantCulture));
								break;
							case ParquetColumnModel.FieldType.Long:
									Columns[index].AddLong(Convert.ToInt64(value, CultureInfo.InvariantCulture));
								break;
							case ParquetColumnModel.FieldType.Guid:
									Columns[index].AddGuid(ConvertToGuid(value));
								break;
							case ParquetColumnModel.FieldType.Time:
									Columns[index].AddTime(ConvertToTimeSpan(value));
								break;
							case ParquetColumnModel.FieldType.ByteArray:
									Columns[index].AddByteArray((byte[]) value);
								break;
							default:
									Columns[index].AddString(Convert.ToString(value, CultureInfo.InvariantCulture));
								break;
						}
			}
	}

	/// <summary>
	///		Convierte un valor a <see cref="DateTime"/> admitiendo también <see cref="DateOnly"/> y <see cref="DateTimeOffset"/>
	/// </summary>
	private static DateTime ConvertToDateTime(object value)
	{
		return value switch
			{
				DateTime dateTime => dateTime,
				DateOnly dateOnly => dateOnly.ToDateTime(TimeOnly.MinValue),
				DateTimeOffset dateTimeOffset => dateTimeOffset.UtcDateTime,
				string text => DateTime.Parse(text, CultureInfo.InvariantCulture),
				_ => Convert.ToDateTime(value, CultureInfo.InvariantCulture)
			};
	}

	/// <summary>
	///		Convierte un valor a <see cref="TimeSpan"/> admitiendo también <see cref="TimeOnly"/>
	/// </summary>
	private static TimeSpan ConvertToTimeSpan(object value)
	{
		return value switch
			{
				TimeSpan timeSpan => timeSpan,
				TimeOnly timeOnly => timeOnly.ToTimeSpan(),
				string text => TimeSpan.Parse(text, CultureInfo.InvariantCulture),
				_ => throw new InvalidCastException($"Can't convert the value '{value}' ({value.GetType().Name}) to TimeSpan")
			};
	}

	/// <summary>
	///		Convierte un valor a <see cref="Guid"/> admitiendo también cadenas y matrices de 16 bytes
	/// </summary>
	private static Guid ConvertToGuid(object value)
	{
		return value switch
			{
				Guid guid => guid,
				string text => Guid.Parse(text),
				byte[] bytes => new Guid(bytes),
				_ => throw new InvalidCastException($"Can't convert the value '{value}' ({value.GetType().Name}) to Guid")
			};
	}

	/// <summary>
	///		Libera la caché
	/// </summary>
	public async Task FlushAsync(CancellationToken cancellationToken)
	{
		// Graba los datos que tenía en memoria
		if (Writer != null && Columns.Count > 0 && Columns[0].Count > 0)
			using (ParquetRowGroupWriter groupWriter = Writer.CreateRowGroup())
			{
				for (int index = 0; index < Columns.Count; index++)
					await Columns[index].WriteToAsync(groupWriter, cancellationToken);
				groupWriter.CompleteValidate();
			}
		// Limpia los valores que había hasta ahora
		for (int index = 0; index < Columns.Count; index++)
			Columns[index].Clear();
	}

	/// <summary>
	///		Libera la memoria
	/// </summary>
	public virtual async ValueTask DisposeAsync()
	{
		if (!Disposed)
		{
			// Si se está liberando la memoria
			if (Writer != null)
			{
				// Envía los datos sobrantes
				await FlushAsync(CancellationToken.None);
				// Libera el stream
				//? Se tiene que hacer primero el Dispose del Writer para que la librería escriba el pie del archivo, no se puede ponerlo a null directamente
				await Writer.DisposeAsync();
				Writer = null;
			}
			// Liberar los recursos no administrados (objetos no administrados) y reemplazar el finalizador
			// Indica que se ha liberado la memoria
			Disposed = true;
		}
	}

	/// <summary>
	///		Columnas del archivo
	/// </summary>
	private List<ParquetColumnModel> Columns { get; } = [];

	/// <summary>
	///		Generador del archivo parquet
	/// </summary>
	private ParquetWriter? Writer { get; set; }

	/// <summary>
	///		Tamaño del grupo de filas
	/// </summary>
	private int RowGroupSize { get; }

	/// <summary>
	///		Número de registros en la caché de escritura
	/// </summary>
	public int Records => Columns.Count == 0 ? 0 : Columns[0].Count;

	/// <summary>
	///		Indica si se ha liberado la memoria
	/// </summary>
	public bool Disposed { get; private set; }
}