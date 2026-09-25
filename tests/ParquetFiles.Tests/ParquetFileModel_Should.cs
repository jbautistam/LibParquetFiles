using System.Data;
using FluentAssertions;
using Bau.Libraries.LibParquetFiles.Writers.Models;
using Parquet;
using ParquetFiles.Tests.MotherObject;

namespace ParquetFiles.Tests;

/// <summary>
///		<see cref="IDataReader"/> que miente deliberadamente: <see cref="IsDBNull"/> siempre devuelve false aunque
///	<see cref="GetValue"/> devuelva <see cref="DBNull"/>, para probar la comprobación defensiva de
///	<see cref="ParquetFileModel"/> que no se fía únicamente de IsDBNull
/// </summary>
file sealed class QuirkyNullReader : IDataReader
{
	private bool _read;

	public void Close() { }
	public void Dispose() { }
	public DataTable? GetSchemaTable() => throw new NotImplementedException();
	public bool NextResult() => false;
	public bool Read()
	{
		if (_read)
			return false;
		_read = true;
		return true;
	}
	public int Depth => 0;
	public bool IsClosed => false;
	public int RecordsAffected => -1;
	public bool GetBoolean(int i) => throw new NotImplementedException();
	public byte GetByte(int i) => throw new NotImplementedException();
	public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) => throw new NotImplementedException();
	public char GetChar(int i) => throw new NotImplementedException();
	public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) => throw new NotImplementedException();
	public IDataReader GetData(int i) => throw new NotImplementedException();
	public string GetDataTypeName(int i) => "Int32";
	public DateTime GetDateTime(int i) => throw new NotImplementedException();
	public decimal GetDecimal(int i) => throw new NotImplementedException();
	public double GetDouble(int i) => throw new NotImplementedException();
	public Type GetFieldType(int i) => typeof(int);
	public float GetFloat(int i) => throw new NotImplementedException();
	public Guid GetGuid(int i) => throw new NotImplementedException();
	public short GetInt16(int i) => throw new NotImplementedException();
	public int GetInt32(int i) => throw new NotImplementedException();
	public long GetInt64(int i) => throw new NotImplementedException();
	public string GetName(int i) => "Value";
	public int GetOrdinal(string name) => 0;
	public string GetString(int i) => throw new NotImplementedException();
	public object GetValue(int i) => DBNull.Value;
	public int GetValues(object[] values) => throw new NotImplementedException();
	public bool IsDBNull(int i) => false;
	public int FieldCount => 1;
	public object this[int i] => GetValue(i);
	public object this[string name] => GetValue(0);
}

/// <summary>
///		Regresión para el bug de mapeo de tipos por subcadena: antes de la corrección, cualquier tipo cuyo nombre
///	completo contuviera ".int", ".date" o ".bool" como subcadena (no como coincidencia exacta) se clasificaba mal.
///	El nombre de la clase, no un espacio de nombres artificial, ya reproduce el patrón porque el separador entre el
///	espacio de nombres y el nombre de tipo es un punto
/// </summary>
file sealed class IntegrationRecord { }
file sealed class DatesRange { }
file sealed class Boolish { }
file sealed class GuidoSomething { }

/// <summary>
///		Pruebas de <see cref="ParquetFileModel"/> (clase interna, accesible gracias a InternalsVisibleTo)
/// </summary>
public class ParquetFileModel_Should
{
	private enum Suit { Clubs, Diamonds, Hearts, Spades }

	private enum ByteBackedEnum : byte { A, B }

	/// <summary>
	///		Tabla completa de mapeo de tipo CLR a <see cref="ParquetColumnModel.FieldType"/>
	/// </summary>
	public static IEnumerable<object[]> TypeMappings()
	{
		yield return new object[] { typeof(bool), ParquetColumnModel.FieldType.Boolean };
		yield return new object[] { typeof(bool?), ParquetColumnModel.FieldType.Boolean };
		yield return new object[] { typeof(byte), ParquetColumnModel.FieldType.Byte };
		yield return new object[] { typeof(sbyte), ParquetColumnModel.FieldType.Integer };
		yield return new object[] { typeof(short), ParquetColumnModel.FieldType.Integer };
		yield return new object[] { typeof(ushort), ParquetColumnModel.FieldType.Integer };
		yield return new object[] { typeof(int), ParquetColumnModel.FieldType.Integer };
		yield return new object[] { typeof(int?), ParquetColumnModel.FieldType.Integer };
		yield return new object[] { typeof(uint), ParquetColumnModel.FieldType.Long };
		yield return new object[] { typeof(long), ParquetColumnModel.FieldType.Long };
		yield return new object[] { typeof(ulong), ParquetColumnModel.FieldType.Decimal };
		yield return new object[] { typeof(decimal), ParquetColumnModel.FieldType.Decimal };
		yield return new object[] { typeof(float), ParquetColumnModel.FieldType.Double };
		yield return new object[] { typeof(double), ParquetColumnModel.FieldType.Double };
		yield return new object[] { typeof(DateTime), ParquetColumnModel.FieldType.DateTime };
		yield return new object[] { typeof(DateOnly), ParquetColumnModel.FieldType.DateTime };
		yield return new object[] { typeof(DateTimeOffset), ParquetColumnModel.FieldType.DateTime };
		yield return new object[] { typeof(TimeSpan), ParquetColumnModel.FieldType.Time };
		yield return new object[] { typeof(TimeOnly), ParquetColumnModel.FieldType.Time };
		yield return new object[] { typeof(Guid), ParquetColumnModel.FieldType.Guid };
		yield return new object[] { typeof(Guid?), ParquetColumnModel.FieldType.Guid };
		yield return new object[] { typeof(byte[]), ParquetColumnModel.FieldType.ByteArray };
		yield return new object[] { typeof(string), ParquetColumnModel.FieldType.String };
		yield return new object[] { typeof(char), ParquetColumnModel.FieldType.String };
		yield return new object[] { typeof(object), ParquetColumnModel.FieldType.String };
		yield return new object[] { typeof(Suit), ParquetColumnModel.FieldType.Integer };
		yield return new object[] { typeof(ByteBackedEnum), ParquetColumnModel.FieldType.Byte };
	}

	/// <summary>
	///		Comprueba el mapeo de cada tipo CLR de la tabla anterior
	/// </summary>
	[Theory]
	[MemberData(nameof(TypeMappings))]
	public void map_clr_type_to_the_expected_field_type(Type clrType, object expected)
	{
		ParquetFileModel model = new(10);

			model.GetColumnSchemaType(clrType).Should().Be((ParquetColumnModel.FieldType) expected);
	}

	/// <summary>
	///		Comprueba que un tipo cuyo nombre contiene "int", "date" o "bool" como subcadena (sin ser ese su tipo)
	///	no se clasifica por error como Integer, DateTime o Boolean
	/// </summary>
	[Fact]
	public void not_misclassify_types_whose_name_contains_a_type_keyword_as_a_substring()
	{
		ParquetFileModel model = new(10);

			model.GetColumnSchemaType(typeof(IntegrationRecord)).Should().Be(ParquetColumnModel.FieldType.String);
			model.GetColumnSchemaType(typeof(DatesRange)).Should().Be(ParquetColumnModel.FieldType.String);
			model.GetColumnSchemaType(typeof(Boolish)).Should().Be(ParquetColumnModel.FieldType.String);
			model.GetColumnSchemaType(typeof(GuidoSomething)).Should().Be(ParquetColumnModel.FieldType.String);
	}

	/// <summary>
	///		Comprueba que no hay ningún registro en la caché antes de abrir el archivo (evita el
	///	IndexOutOfRangeException que lanzaba <c>Columns[0].Count</c> cuando no había columnas)
	/// </summary>
	[Fact]
	public void return_zero_records_before_the_file_is_open()
	{
		ParquetFileModel model = new(10);

			model.Records.Should().Be(0);
	}

	/// <summary>
	///		Comprueba que abrir el archivo con un <see cref="System.Data.IDataReader"/> sin columnas lanza en lugar
	///	de fallar dentro de la creación del esquema de Parquet.Net
	/// </summary>
	[Fact]
	public async Task throw_when_the_reader_has_no_columns()
	{
		using MemoryStream stream = new();
		FakeDataReader reader = new(Array.Empty<(string, Type)>(), new List<object?[]>());
		ParquetFileModel model = new(10);

			await Assert.ThrowsAsync<ArgumentException>(() => model.OpenAsync(stream, reader, TestContext.Current.CancellationToken));
	}

	/// <summary>
	///		Comprueba que las columnas sin nombre se llaman "Column" seguido de su índice
	/// </summary>
	[Fact]
	public async Task use_the_column_index_when_the_column_name_is_empty()
	{
		using MemoryStream stream = new();
		FakeDataReader reader = FakeDataReader.CreateSingleColumn("", new int?[] { 1 });

			await using (ParquetFileModel model = new(10))
			{
				await model.OpenAsync(stream, reader, TestContext.Current.CancellationToken);
				while (reader.Read())
					await model.WriteRecordAsync(reader, TestContext.Current.CancellationToken);
			}
			stream.Position = 0;

			await using ParquetReader parquetReader = await ParquetReader.CreateAsync(stream, cancellationToken: TestContext.Current.CancellationToken);

				parquetReader.Schema.GetDataFields()[0].Name.Should().Be("Column0");
	}

	/// <summary>
	///		Comprueba que un valor se trata como nulo aunque IsDBNull mienta y devuelva false, siempre que GetValue
	///	devuelva realmente <see cref="DBNull"/> (comprobación defensiva que no se fía de un único origen de verdad)
	/// </summary>
	[Fact]
	public async Task treat_a_value_as_null_when_get_value_returns_dbnull_even_if_is_db_null_lies()
	{
		using MemoryStream stream = new();
		QuirkyNullReader reader = new();

			await using (ParquetFileModel model = new(10))
			{
				await model.OpenAsync(stream, reader, TestContext.Current.CancellationToken);
				while (reader.Read())
					await model.WriteRecordAsync(reader, TestContext.Current.CancellationToken);
			}
			stream.Position = 0;

			using (Bau.Libraries.LibParquetFiles.Readers.ParquetDataReader verify = new())
			{
				await verify.OpenAsync(stream, TestContext.Current.CancellationToken);
				await verify.ReadAsync(TestContext.Current.CancellationToken);

					verify.IsDBNull(0).Should().BeTrue();
			}
	}
}
