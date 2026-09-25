using System.Data;
using FluentAssertions;
using Bau.Libraries.LibParquetFiles.Readers;
using Bau.Libraries.LibParquetFiles.Writers;
using Parquet;
using Parquet.Schema;
using ParquetFiles.Tests.MotherObject;

namespace ParquetFiles.Tests;

/// <summary>
///		Pruebas de <see cref="ParquetDataReader"/>
/// </summary>
public class ParquetDataReader_Should
{
	/// <summary>
	///		Comprueba que el esquema está disponible nada más abrir el archivo, sin haber leido ninguna fila todavía
	///	(antes de la corrección, GetName/GetFieldType/GetDataTypeName daban un NullReferenceException en este punto)
	/// </summary>
	[Fact]
	public async Task expose_the_schema_before_reading_any_row()
	{
		using TempFile file = new();

			await WriteSampleFileAsync(file.FileName);
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);

					reader.FieldCount.Should().Be(2);
					reader.GetName(0).Should().Be("Id");
					reader.GetFieldType(0).Should().Be(typeof(int));
					reader.GetDataTypeName(0).Should().Be("Int32");
			}
	}

	/// <summary>
	///		Comprueba que GetOrdinal no distingue mayúsculas de minúsculas
	/// </summary>
	[Fact]
	public async Task resolve_ordinals_ignoring_case()
	{
		using TempFile file = new();

			await WriteSampleFileAsync(file.FileName);
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);

					reader.GetOrdinal("id").Should().Be(0);
					reader.GetOrdinal("NAME").Should().Be(1);
			}
	}

	/// <summary>
	///		Comprueba que un nombre de campo desconocido, vacío o nulo devuelve -1 en lugar de lanzar
	/// </summary>
	[Theory]
	[InlineData("Unknown")]
	[InlineData("")]
	[InlineData(null)]
	public async Task return_minus_one_for_an_unknown_ordinal(string? name)
	{
		using TempFile file = new();

			await WriteSampleFileAsync(file.FileName);
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);

					reader.GetOrdinal(name!).Should().Be(-1);
			}
	}

	/// <summary>
	///		Comprueba que se leen todas las filas, atravesando varios grupos de filas
	/// </summary>
	[Fact]
	public async Task read_all_rows_across_row_groups()
	{
		(string, Type)[] columns = { ("Value", typeof(int)) };
		List<object?[]> data = Enumerable.Range(0, 55).Select(i => new object?[] { i }).ToList();

		using TempFile file = new();

			using (FakeDataReader source = new(columns, data))
			{
				ParquetDataWriter writer = new(20);

					await writer.WriteAsync(file.FileName, source, TestContext.Current.CancellationToken);
			}
			using (ParquetDataReader reader = new())
			{
				List<int> values = new();

					await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
					while (await reader.ReadAsync(TestContext.Current.CancellationToken))
						values.Add((int) reader.GetValue(0));
					values.Should().Equal(data.Select(row => (int) row[0]!));
			}
	}

	/// <summary>
	///		Comprueba que Read() (la versión síncrona) funciona igual que ReadAsync
	/// </summary>
	[Fact]
	public async Task read_synchronously_with_read()
	{
		using TempFile file = new();

			await WriteSampleFileAsync(file.FileName, 3);
			using (ParquetDataReader reader = new())
			{
				int count = 0;

					await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
					while (reader.Read())
						count++;
					count.Should().Be(3);
			}
	}

	/// <summary>
	///		Comprueba que se puede abrir el archivo a partir de un <see cref="Stream"/> en lugar de una ruta
	/// </summary>
	[Fact]
	public async Task open_from_a_stream()
	{
		using TempFile file = new();

			await WriteSampleFileAsync(file.FileName, 2);
			using (ParquetDataReader reader = new())
			{
				int count = 0;
				Stream stream = File.OpenRead(file.FileName);

					// El reader toma posesión del stream (lo cierra en Close/Dispose)
					await reader.OpenAsync(stream, TestContext.Current.CancellationToken);
					while (await reader.ReadAsync(TestContext.Current.CancellationToken))
						count++;
					count.Should().Be(2);
			}
	}

	/// <summary>
	///		Comprueba que, al llegar al final del archivo, sucesivas llamadas a ReadAsync devuelven false de forma
	///	idempotente
	/// </summary>
	[Fact]
	public async Task return_false_when_there_is_nothing_more_to_read()
	{
		using TempFile file = new();

			await WriteSampleFileAsync(file.FileName, 1);
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);

					(await reader.ReadAsync(TestContext.Current.CancellationToken)).Should().BeTrue();
					(await reader.ReadAsync(TestContext.Current.CancellationToken)).Should().BeFalse();
					(await reader.ReadAsync(TestContext.Current.CancellationToken)).Should().BeFalse();
			}
	}

	/// <summary>
	///		Comprueba que un grupo de filas vacío intercalado entre otros con datos se salta correctamente (el
	///	archivo se genera con la API cruda de Parquet.Net porque nuestro propio escritor nunca produce grupos vacíos)
	/// </summary>
	[Fact]
	public async Task skip_empty_row_groups()
	{
		using TempFile file = new();

			DataField<int?> field = new("Value");
			ParquetSchema schema = new(field);

			using (Stream stream = File.Create(file.FileName))
			await using (ParquetWriter writer = await ParquetWriter.CreateAsync(schema, stream, cancellationToken: TestContext.Current.CancellationToken))
			{
				using (ParquetRowGroupWriter g1 = writer.CreateRowGroup())
					await g1.WriteAsync<int>(field, new int?[] { 1, 2, 3 }.AsMemory(), cancellationToken: TestContext.Current.CancellationToken);
				using (ParquetRowGroupWriter g2 = writer.CreateRowGroup())
					await g2.WriteAsync<int>(field, Array.Empty<int?>().AsMemory(), cancellationToken: TestContext.Current.CancellationToken);
				using (ParquetRowGroupWriter g3 = writer.CreateRowGroup())
					await g3.WriteAsync<int>(field, new int?[] { 4, 5 }.AsMemory(), cancellationToken: TestContext.Current.CancellationToken);
			}
			using (ParquetDataReader reader = new())
			{
				List<int> values = new();

					await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
					while (await reader.ReadAsync(TestContext.Current.CancellationToken))
						values.Add((int) reader.GetValue(0));
					values.Should().Equal(1, 2, 3, 4, 5);
			}
	}

	/// <summary>
	///		Comprueba tipos y la variante no anulable que nuestro propio escritor nunca produce (sólo tiene 11 tipos
	///	de columna posibles, todos anulables), pero que el lector sí admite al leer un archivo generado por otra
	///	herramienta: sbyte, ushort, uint, ulong, float y una columna entera no anulable.
	///		<see cref="DateOnly"/> y <see cref="TimeOnly"/> se han probado aparte (comprobado experimentalmente): al
	///	releer el esquema, Parquet.Net normaliza sus campos DATE/TIME a <see cref="DateTime"/>/<see cref="TimeSpan"/>
	///	sin importar el tipo genérico usado al escribir, así que las ramas DateOnly/TimeOnly de ReadColumnAsync son
	///	inalcanzables con un archivo real; se dejan como salvaguarda por si Parquet.Net cambiase ese comportamiento
	/// </summary>
	[Fact]
	public async Task read_types_and_nullability_not_produced_by_our_own_writer()
	{
		using TempFile file = new();

			DataField<int> requiredInt = new("RequiredInt");
			DataField<short?> shortField = new("Short");
			DataField<sbyte?> sbyteField = new("SByte");
			DataField<ushort?> ushortField = new("UShort");
			DataField<uint?> uintField = new("UInt");
			DataField<ulong?> ulongField = new("ULong");
			DataField<float?> floatField = new("Float");
			ParquetSchema schema = new(requiredInt, shortField, sbyteField, ushortField, uintField, ulongField, floatField);

			using (Stream stream = File.Create(file.FileName))
			await using (ParquetWriter writer = await ParquetWriter.CreateAsync(schema, stream, cancellationToken: TestContext.Current.CancellationToken))
			{
				using ParquetRowGroupWriter groupWriter = writer.CreateRowGroup();

					await groupWriter.WriteAsync<int>(requiredInt, new int[] { 7 }.AsMemory(), cancellationToken: TestContext.Current.CancellationToken);
					await groupWriter.WriteAsync<short>(shortField, new short?[] { -300 }.AsMemory(), cancellationToken: TestContext.Current.CancellationToken);
					await groupWriter.WriteAsync<sbyte>(sbyteField, new sbyte?[] { -12 }.AsMemory(), cancellationToken: TestContext.Current.CancellationToken);
					await groupWriter.WriteAsync<ushort>(ushortField, new ushort?[] { 60_000 }.AsMemory(), cancellationToken: TestContext.Current.CancellationToken);
					await groupWriter.WriteAsync<uint>(uintField, new uint?[] { 4_000_000_000 }.AsMemory(), cancellationToken: TestContext.Current.CancellationToken);
					await groupWriter.WriteAsync<ulong>(ulongField, new ulong?[] { 18_000_000_000_000_000_000 }.AsMemory(), cancellationToken: TestContext.Current.CancellationToken);
					await groupWriter.WriteAsync<float>(floatField, new float?[] { 1.5f }.AsMemory(), cancellationToken: TestContext.Current.CancellationToken);
			}
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
				await reader.ReadAsync(TestContext.Current.CancellationToken);

					reader.GetValue(reader.GetOrdinal("RequiredInt")).Should().Be(7);
					reader.GetValue(reader.GetOrdinal("Short")).Should().Be((short) -300);
					reader.GetValue(reader.GetOrdinal("SByte")).Should().Be((sbyte) -12);
					reader.GetValue(reader.GetOrdinal("UShort")).Should().Be((ushort) 60_000);
					reader.GetValue(reader.GetOrdinal("UInt")).Should().Be((uint) 4_000_000_000);
					reader.GetValue(reader.GetOrdinal("ULong")).Should().Be((ulong) 18_000_000_000_000_000_000);
					reader.GetValue(reader.GetOrdinal("Float")).Should().Be(1.5f);
			}
	}

	/// <summary>
	///		Comprueba que acceder al esquema antes de abrir el archivo lanza en lugar de un NullReferenceException
	/// </summary>
	[Fact]
	public void throw_when_accessing_the_schema_before_opening()
	{
		ParquetDataReader reader = new();

			Action act = () => reader.GetName(0);

			act.Should().Throw<InvalidOperationException>();
	}

	/// <summary>
	///		Comprueba que GetData lanza NotSupportedException (Parquet no tiene resultsets anidados)
	/// </summary>
	[Fact]
	public async Task throw_not_supported_from_get_data()
	{
		using TempFile file = new();

			await WriteSampleFileAsync(file.FileName, 1);
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
				await reader.ReadAsync(TestContext.Current.CancellationToken);

					Action act = () => reader.GetData(0);

					act.Should().Throw<NotSupportedException>();
			}
	}

	/// <summary>
	///		Comprueba GetChar sobre una cadena de un único carácter, y que lanza si tiene más de uno
	/// </summary>
	[Fact]
	public async Task get_char_from_a_single_character_string_and_throw_otherwise()
	{
		(string, Type)[] columns = { ("Letter", typeof(string)), ("Word", typeof(string)) };

		using TempFile file = new();

			using (FakeDataReader source = new(columns, new List<object?[]> { new object?[] { "A", "AB" } }))
			{
				ParquetDataWriter writer = new(10);

					await writer.WriteAsync(file.FileName, source, TestContext.Current.CancellationToken);
			}
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
				await reader.ReadAsync(TestContext.Current.CancellationToken);

					reader.GetChar(reader.GetOrdinal("Letter")).Should().Be('A');

					Action act = () => reader.GetChar(reader.GetOrdinal("Word"));

					act.Should().Throw<InvalidCastException>();
			}
	}

	/// <summary>
	///		Comprueba GetChars: consultar la longitud total sin buffer, y copiar un tramo con buffer
	/// </summary>
	[Fact]
	public async Task copy_a_range_of_characters_with_get_chars()
	{
		(string, Type)[] columns = { ("Text", typeof(string)) };

		using TempFile file = new();

			using (FakeDataReader source = new(columns, new List<object?[]> { new object?[] { "Hello world" } }))
			{
				ParquetDataWriter writer = new(10);

					await writer.WriteAsync(file.FileName, source, TestContext.Current.CancellationToken);
			}
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
				await reader.ReadAsync(TestContext.Current.CancellationToken);

					reader.GetChars(0, 0, null, 0, 0).Should().Be(11);

					char[] buffer = new char[5];
					long copied = reader.GetChars(0, 6, buffer, 0, 5);

					copied.Should().Be(5);
					new string(buffer).Should().Be("world");
			}
	}

	/// <summary>
	///		Comprueba que GetGuid lanza si el valor no es un Guid
	/// </summary>
	[Fact]
	public async Task throw_when_get_guid_value_is_not_a_guid()
	{
		using TempFile file = new();

			await WriteSampleFileAsync(file.FileName, 1);
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
				await reader.ReadAsync(TestContext.Current.CancellationToken);

					Action act = () => reader.GetGuid(0);

					act.Should().Throw<InvalidCastException>();
			}
	}

	/// <summary>
	///		Comprueba que GetString lanza si el valor no es una cadena
	/// </summary>
	[Fact]
	public async Task throw_when_get_string_value_is_not_a_string()
	{
		using TempFile file = new();

			await WriteSampleFileAsync(file.FileName, 1);
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
				await reader.ReadAsync(TestContext.Current.CancellationToken);

					Action act = () => reader.GetString(0);

					act.Should().Throw<InvalidCastException>();
			}
	}

	/// <summary>
	///		Comprueba que un getter tipado sobre un valor nulo lanza InvalidCastException en lugar de convertir
	///	silenciosamente <see cref="DBNull"/>
	/// </summary>
	[Fact]
	public async Task throw_when_a_typed_getter_is_called_on_a_null_value()
	{
		(string, Type)[] columns = { ("Value", typeof(int)) };

		using TempFile file = new();

			using (FakeDataReader source = new(columns, new List<object?[]> { new object?[] { null } }))
			{
				ParquetDataWriter writer = new(10);

					await writer.WriteAsync(file.FileName, source, TestContext.Current.CancellationToken);
			}
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
				await reader.ReadAsync(TestContext.Current.CancellationToken);

					Action act = () => reader.GetInt32(0);

					act.Should().Throw<InvalidCastException>();
			}
	}

	/// <summary>
	///		Comprueba todos los getters tipados, incluidas las conversiones de ensanchamiento legítimas
	///	(por ejemplo, una columna short se escribe como Integer y GetInt16 debe volver a estrecharla)
	/// </summary>
	[Fact]
	public async Task expose_every_typed_getter()
	{
		Guid guid = Guid.Parse("DC340CF2-331E-4B58-9F96-B5009EAA8987");
		(string, Type)[] columns =
			{
				("Bool", typeof(bool)), ("Byte", typeof(byte)), ("Short", typeof(short)), ("Int", typeof(int)),
				("Long", typeof(long)), ("Float", typeof(float)), ("Double", typeof(double)), ("Decimal", typeof(decimal)),
				("String", typeof(string)), ("DateTime", typeof(DateTime)), ("Guid", typeof(Guid))
			};
		object?[] row = { true, (byte) 1, (short) 2, 3, 4L, 5f, 6.0, 7.5m, "text", new DateTime(2024, 1, 1), guid };

		using TempFile file = new();

			using (FakeDataReader source = new(columns, new List<object?[]> { row }))
			{
				ParquetDataWriter writer = new(10);

					await writer.WriteAsync(file.FileName, source, TestContext.Current.CancellationToken);
			}
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
				await reader.ReadAsync(TestContext.Current.CancellationToken);

					reader.GetBoolean(reader.GetOrdinal("Bool")).Should().BeTrue();
					reader.GetByte(reader.GetOrdinal("Byte")).Should().Be(1);
					reader.GetInt16(reader.GetOrdinal("Short")).Should().Be(2);
					reader.GetInt32(reader.GetOrdinal("Int")).Should().Be(3);
					reader.GetInt64(reader.GetOrdinal("Long")).Should().Be(4L);
					reader.GetFloat(reader.GetOrdinal("Float")).Should().Be(5f);
					reader.GetDouble(reader.GetOrdinal("Double")).Should().Be(6.0);
					reader.GetDecimal(reader.GetOrdinal("Decimal")).Should().Be(7.5m);
					reader.GetString(reader.GetOrdinal("String")).Should().Be("text");
					reader.GetDateTime(reader.GetOrdinal("DateTime")).Should().Be(new DateTime(2024, 1, 1));
					reader.GetGuid(reader.GetOrdinal("Guid")).Should().Be(guid);
			}
	}

	/// <summary>
	///		Comprueba que GetValues rellena una matriz con los valores de la fila actual
	/// </summary>
	[Fact]
	public async Task fill_an_array_with_get_values()
	{
		using TempFile file = new();

			await WriteSampleFileAsync(file.FileName, 1);
			using (ParquetDataReader reader = new())
			{
				object[] values = new object[5];

					await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
					await reader.ReadAsync(TestContext.Current.CancellationToken);

					int count = reader.GetValues(values);

					count.Should().Be(2);
					values[0].Should().Be(0);
					values[1].Should().Be("Name0");
			}
	}

	/// <summary>
	///		Comprueba GetBytes: consultar la longitud total sin buffer, y copiar un tramo con buffer
	/// </summary>
	[Fact]
	public async Task stream_bytes_with_get_bytes()
	{
		(string, Type)[] columns = { ("Data", typeof(byte[])) };
		byte[] original = { 1, 2, 3, 4, 5 };

		using TempFile file = new();

			using (FakeDataReader source = new(columns, new List<object?[]> { new object?[] { original } }))
			{
				ParquetDataWriter writer = new(10);

					await writer.WriteAsync(file.FileName, source, TestContext.Current.CancellationToken);
			}
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
				await reader.ReadAsync(TestContext.Current.CancellationToken);

					reader.GetBytes(0, 0, null, 0, 0).Should().Be(5);

					byte[] buffer = new byte[3];
					long copied = reader.GetBytes(0, 1, buffer, 0, 3);

					copied.Should().Be(3);
					buffer.Should().Equal(new byte[] { 2, 3, 4 });
			}
	}

	/// <summary>
	///		Comprueba que GetSchemaTable construye una tabla ADO.NET mínima con el esquema del archivo
	/// </summary>
	[Fact]
	public async Task build_a_schema_table()
	{
		using TempFile file = new();

			await WriteSampleFileAsync(file.FileName, 1);
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);

					DataTable schemaTable = reader.GetSchemaTable();

					schemaTable.Rows.Count.Should().Be(2);
					schemaTable.Rows[0]["ColumnName"].Should().Be("Id");
					schemaTable.Rows[1]["ColumnName"].Should().Be("Name");
			}
	}

	/// <summary>
	///		Comprueba que el reader se puede usar con DataTable.Load, una integración habitual de ADO.NET que antes
	///	no funcionaba porque los getters tipados lanzaban NotImplementedException
	/// </summary>
	[Fact]
	public async Task be_usable_with_data_table_load()
	{
		using TempFile file = new();

			await WriteSampleFileAsync(file.FileName, 3);
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);

					DataTable table = new();

						table.Load(reader);
						table.Rows.Count.Should().Be(3);
						table.Columns.Count.Should().Be(2);
			}
	}

	/// <summary>
	///		Comprueba que un valor nulo se expone como <see cref="DBNull"/>, tanto por GetValue como por los
	///	indexadores, en lugar de como null (que rompía el contrato de IDataReader)
	/// </summary>
	[Fact]
	public async Task return_dbnull_for_null_values()
	{
		(string, Type)[] columns = { ("Value", typeof(int)) };
		List<object?[]> data = new() { new object?[] { null } };

		using TempFile file = new();

			using (FakeDataReader source = new(columns, data))
			{
				ParquetDataWriter writer = new(10);

					await writer.WriteAsync(file.FileName, source, TestContext.Current.CancellationToken);
			}
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
				await reader.ReadAsync(TestContext.Current.CancellationToken);

					reader.IsDBNull(0).Should().BeTrue();
					reader.GetValue(0).Should().Be(DBNull.Value);
					reader[0].Should().Be(DBNull.Value);
			}
	}

	/// <summary>
	///		Comprueba IsClosed antes de abrir y después de cerrar
	/// </summary>
	[Fact]
	public async Task report_is_closed_before_open_and_after_close()
	{
		using TempFile file = new();

			await WriteSampleFileAsync(file.FileName, 1);

			ParquetDataReader reader = new();

				reader.IsClosed.Should().BeTrue();
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
				reader.IsClosed.Should().BeFalse();
				reader.Close();
				reader.IsClosed.Should().BeTrue();
	}

	/// <summary>
	///		Comprueba las propiedades fijas del contrato de IDataReader que este lector no usa realmente
	/// </summary>
	[Fact]
	public void expose_depth_zero_and_records_affected_minus_one()
	{
		ParquetDataReader reader = new();

			reader.Depth.Should().Be(0);
			reader.RecordsAffected.Should().Be(-1);
	}

	/// <summary>
	///		Comprueba que NextResult siempre devuelve false (Parquet no tiene varios resultsets)
	/// </summary>
	[Fact]
	public async Task return_false_from_next_result()
	{
		using TempFile file = new();

			await WriteSampleFileAsync(file.FileName, 1);
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);

					reader.NextResult().Should().BeFalse();
			}
	}

	/// <summary>
	///		Comprueba que se puede llamar a Dispose dos veces sin que lance
	/// </summary>
	[Fact]
	public async Task be_safe_to_dispose_twice()
	{
		using TempFile file = new();

			await WriteSampleFileAsync(file.FileName, 1);

			ParquetDataReader reader = new();

				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
				reader.Dispose();

				Action act = reader.Dispose;

					act.Should().NotThrow();
					reader.Disposed.Should().BeTrue();
	}

	/// <summary>
	///		Comprueba que un NotifyAfter de cero no provoca un DivideByZeroException (el guard sólo estaba dentro de
	///	RaiseEventReadBlock, no antes del módulo del contador de filas)
	/// </summary>
	[Fact]
	public async Task not_throw_when_notify_after_is_zero()
	{
		using TempFile file = new();

			await WriteSampleFileAsync(file.FileName, 5);
			using (ParquetDataReader reader = new(notifyAfter: 0))
			{
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);

					Func<Task> act = async () =>
						{
							while (await reader.ReadAsync(TestContext.Current.CancellationToken))
							{
								// Sólo se recorren las filas
							}
						};

					await act.Should().NotThrowAsync();
			}
	}

	/// <summary>
	///		Comprueba que la misma instancia se puede reutilizar para abrir un segundo archivo con un esquema
	///	distinto (antes, OpenAsync no reiniciaba el esquema ni cerraba el archivo anterior)
	/// </summary>
	[Fact]
	public async Task be_reusable_for_a_second_file_with_a_different_schema()
	{
		using TempFile firstFile = new();
		using TempFile secondFile = new();

			await WriteSampleFileAsync(firstFile.FileName, 2);

			(string, Type)[] otherColumns = { ("OtherValue", typeof(double)) };
			List<object?[]> otherData = new() { new object?[] { 1.5 }, new object?[] { 2.5 }, new object?[] { 3.5 } };

			using (FakeDataReader source = new(otherColumns, otherData))
			{
				ParquetDataWriter writer = new(10);

					await writer.WriteAsync(secondFile.FileName, source, TestContext.Current.CancellationToken);
			}
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(firstFile.FileName, TestContext.Current.CancellationToken);
				reader.FieldCount.Should().Be(2);

				await reader.OpenAsync(secondFile.FileName, TestContext.Current.CancellationToken);
				reader.FieldCount.Should().Be(1);
				reader.GetName(0).Should().Be("OtherValue");

				int count = 0;

					while (await reader.ReadAsync(TestContext.Current.CancellationToken))
						count++;
					count.Should().Be(3);
			}
	}

	/// <summary>
	///		Comprueba que, si el archivo no es un parquet válido, OpenAsync no deja el archivo bloqueado
	/// </summary>
	[Fact]
	public async Task not_leave_the_file_locked_when_the_file_is_not_a_parquet()
	{
		using TempFile file = new();

			await File.WriteAllTextAsync(file.FileName, "this is not a parquet file", TestContext.Current.CancellationToken);

			using (ParquetDataReader reader = new())
			{
				Func<Task> act = () => reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);

					await act.Should().ThrowAsync<Exception>();
			}
			// Si el archivo hubiera quedado bloqueado, esto lanzaría una excepción de E/S
			Action reopenAndDelete = () => File.Delete(file.FileName);

				reopenAndDelete.Should().NotThrow();
	}

	/// <summary>
	///		Comprueba que acceder a los valores antes de la primera llamada a Read()/ReadAsync lanza, en lugar de
	///	provocar un NullReferenceException
	/// </summary>
	[Fact]
	public async Task throw_when_reading_values_before_the_first_read()
	{
		using TempFile file = new();

			await WriteSampleFileAsync(file.FileName, 1);
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);

					Action act = () => reader.GetValue(0);

					act.Should().Throw<InvalidOperationException>();
			}
	}

	/// <summary>
	///		Comprueba que el evento Progress se lanza cada NotifyAfter filas leidas
	/// </summary>
	[Fact]
	public async Task raise_the_progress_event_every_notify_after_rows()
	{
		(string, Type)[] columns = { ("Value", typeof(int)) };
		List<object?[]> data = Enumerable.Range(0, 25).Select(i => new object?[] { i }).ToList();

		using TempFile file = new();

			using (FakeDataReader source = new(columns, data))
			{
				ParquetDataWriter writer = new(100);

					await writer.WriteAsync(file.FileName, source, TestContext.Current.CancellationToken);
			}
			using (ParquetDataReader reader = new(notifyAfter: 10))
			{
				List<long> notified = new();

					reader.Progress += (_, e) => notified.Add(e.Records);
					await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
					while (await reader.ReadAsync(TestContext.Current.CancellationToken))
					{
						// Sólo se recorren las filas
					}
					notified.Should().Equal(10, 20);
			}
	}

	/// <summary>
	///		Escribe un archivo de ejemplo con dos columnas (Id: int, Name: string) y el número de filas indicado
	/// </summary>
	private static async Task WriteSampleFileAsync(string fileName, int rows = 3)
	{
		(string, Type)[] columns = { ("Id", typeof(int)), ("Name", typeof(string)) };
		List<object?[]> data = new();

			for (int index = 0; index < rows; index++)
				data.Add(new object?[] { index, $"Name{index}" });

			using FakeDataReader reader = new(columns, data);
			ParquetDataWriter writer = new(1_000);

				await writer.WriteAsync(fileName, reader, TestContext.Current.CancellationToken);
	}
}
