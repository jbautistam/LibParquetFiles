using FluentAssertions;
using Bau.Libraries.LibParquetFiles.Readers;
using Bau.Libraries.LibParquetFiles.Writers;
using ParquetFiles.Tests.MotherObject;

namespace ParquetFiles.Tests;

/// <summary>
///		Pruebas de la matriz completa de tipos CLR admitidos al escribir y volver a leer un archivo parquet
/// </summary>
/// <remarks>
///		Los límites de precisión de <see cref="decimal"/> y <see cref="TimeSpan"/> que se afirman aquí (por ejemplo,
///	que <see cref="TimeSpan"/> sólo conserva precisión de milisegundos, o que un <see cref="decimal"/> de más de unos
///	20 dígitos significativos lanza) se han determinado de forma experimental contra Parquet.Net 6.0.3, no se han
///	adivinado; si se sube de versión Parquet.Net y estas pruebas empiezan a fallar, hay que volver a comprobarlos
/// </remarks>
public class ParquetRoundTrip_Should
{
	private enum Suit { Clubs, Diamonds, Hearts, Spades }

	/// <summary>
	///		Un caso por cada tipo CLR admitido: valores de origen y los valores esperados tras la lectura (declarando
	///	el tipo de llegada, porque hay conversiones legítimas y documentadas, por ejemplo short -&gt; int)
	/// </summary>
	public static IEnumerable<object[]> SupportedTypes()
	{
		yield return new object[] { typeof(bool), new object?[] { true, false }, new object?[] { true, false } };
		yield return new object[] { typeof(byte), new object?[] { byte.MinValue, byte.MaxValue }, new object?[] { byte.MinValue, byte.MaxValue } };
		yield return new object[] { typeof(sbyte), new object?[] { sbyte.MinValue, sbyte.MaxValue }, new object?[] { (int) sbyte.MinValue, (int) sbyte.MaxValue } };
		yield return new object[] { typeof(short), new object?[] { short.MinValue, short.MaxValue }, new object?[] { (int) short.MinValue, (int) short.MaxValue } };
		yield return new object[] { typeof(ushort), new object?[] { ushort.MinValue, ushort.MaxValue }, new object?[] { (int) ushort.MinValue, (int) ushort.MaxValue } };
		yield return new object[] { typeof(int), new object?[] { int.MinValue, int.MaxValue }, new object?[] { int.MinValue, int.MaxValue } };
		yield return new object[] { typeof(uint), new object?[] { uint.MinValue, uint.MaxValue }, new object?[] { (long) uint.MinValue, (long) uint.MaxValue } };
		yield return new object[] { typeof(long), new object?[] { long.MinValue, long.MaxValue }, new object?[] { long.MinValue, long.MaxValue } };
		yield return new object[] { typeof(ulong), new object?[] { ulong.MinValue, 9_999_999_999_999_999_999UL }, new object?[] { (decimal) ulong.MinValue, 9_999_999_999_999_999_999m } };
		yield return new object[] { typeof(float), new object?[] { float.MinValue, float.MaxValue, 0f }, new object?[] { (double) float.MinValue, (double) float.MaxValue, 0d } };
		yield return new object[] { typeof(double), new object?[] { double.MinValue, double.MaxValue }, new object?[] { double.MinValue, double.MaxValue } };
		yield return new object[] { typeof(decimal), new object?[] { 123456789012345.6789m, -0.1m, 0m }, new object?[] { 123456789012345.6789m, -0.1m, 0m } };
		yield return new object[] { typeof(string), new object?[] { "", "Ñoño café 😀🚀 日本語" }, new object?[] { "", "Ñoño café 😀🚀 日本語" } };
		yield return new object[] { typeof(char), new object?[] { 'A', 'ñ' }, new object?[] { "A", "ñ" } };
		yield return new object[] { typeof(byte[]), new object?[] { Array.Empty<byte>(), new byte[] { 1, 2, 3, 255 } }, new object?[] { Array.Empty<byte>(), new byte[] { 1, 2, 3, 255 } } };
		yield return new object[] { typeof(Guid), new object?[] { Guid.Empty, Guid.Parse("DC340CF2-331E-4B58-9F96-B5009EAA8987") }, new object?[] { Guid.Empty, Guid.Parse("DC340CF2-331E-4B58-9F96-B5009EAA8987") } };
		yield return new object[]
			{
				typeof(DateTime),
				new object?[] { new DateTime(2000, 1, 1), new DateTime(2024, 5, 1, 10, 30, 15, 123, DateTimeKind.Unspecified).AddTicks(4567) },
				new object?[] { new DateTime(2000, 1, 1), new DateTime(2024, 5, 1, 10, 30, 15, 123).AddTicks(4567) }
			};
		yield return new object[] { typeof(DateOnly), new object?[] { new DateOnly(2024, 5, 1) }, new object?[] { new DateTime(2024, 5, 1) } };
		yield return new object[]
			{
				typeof(DateTimeOffset),
				new object?[] { new DateTimeOffset(2024, 5, 1, 10, 0, 0, TimeSpan.FromHours(2)) },
				new object?[] { new DateTime(2024, 5, 1, 8, 0, 0) }
			};
		yield return new object[] { typeof(TimeSpan), new object?[] { TimeSpan.Zero, TimeSpan.FromDays(2), -TimeSpan.FromHours(1) }, new object?[] { TimeSpan.Zero, TimeSpan.FromDays(2), -TimeSpan.FromHours(1) } };
		yield return new object[] { typeof(TimeOnly), new object?[] { new TimeOnly(23, 59, 59, 999) }, new object?[] { new TimeSpan(0, 23, 59, 59, 999) } };
		yield return new object[] { typeof(Suit), new object?[] { Suit.Hearts, Suit.Clubs }, new object?[] { (int) Suit.Hearts, (int) Suit.Clubs } };
	}

	/// <summary>
	///		Comprueba que cada tipo CLR admitido se escribe y se vuelve a leer con el valor esperado
	/// </summary>
	[Theory]
	[MemberData(nameof(SupportedTypes))]
	public async Task round_trip_every_supported_clr_type(Type sourceType, object?[] values, object?[] expected)
	{
		using TempFile file = new();

			// Escribe la columna
			using (FakeDataReader reader = FakeDataReader.CreateSingleColumn("Value", sourceType, values))
			{
				ParquetDataWriter writer = new(1_000);

					await writer.WriteAsync(file.FileName, reader, TestContext.Current.CancellationToken);
			}
			// Vuelve a leerla y comprueba los valores
			List<object?> actual = new();

				using (ParquetDataReader reader = new())
				{
					await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
					while (await reader.ReadAsync(TestContext.Current.CancellationToken))
						actual.Add(reader.IsDBNull(0) ? null : reader.GetValue(0));
				}
				actual.Count.Should().Be(expected.Length);
				for (int index = 0; index < expected.Length; index++)
					AssertValueEquals(expected[index], actual[index]);
	}

	/// <summary>
	///		Comprueba que los valores nulos se conservan como <see cref="DBNull"/>, intercalados entre valores válidos,
	///	para cada tipo CLR admitido
	/// </summary>
	[Theory]
	[MemberData(nameof(SupportedTypes))]
	public async Task preserve_null_values_for_every_type(Type sourceType, object?[] values, object?[] expected)
	{
		object?[] valuesWithNulls = new object?[] { null }.Concat(values).Append(null).ToArray();

		using TempFile file = new();

			// Escribe la columna con nulos al principio y al final
			using (FakeDataReader reader = FakeDataReader.CreateSingleColumn("Value", sourceType, valuesWithNulls))
			{
				ParquetDataWriter writer = new(1_000);

					await writer.WriteAsync(file.FileName, reader, TestContext.Current.CancellationToken);
			}
			// Comprueba que los nulos y los valores se han conservado en su sitio
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
				(await reader.ReadAsync(TestContext.Current.CancellationToken)).Should().BeTrue();
				reader.IsDBNull(0).Should().BeTrue();
				reader.GetValue(0).Should().Be(DBNull.Value);
				for (int index = 0; index < expected.Length; index++)
				{
					(await reader.ReadAsync(TestContext.Current.CancellationToken)).Should().BeTrue();
					reader.IsDBNull(0).Should().BeFalse();
					AssertValueEquals(expected[index], reader.GetValue(0));
				}
				(await reader.ReadAsync(TestContext.Current.CancellationToken)).Should().BeTrue();
				reader.IsDBNull(0).Should().BeTrue();
				(await reader.ReadAsync(TestContext.Current.CancellationToken)).Should().BeFalse();
			}
	}

	/// <summary>
	///		Comprueba que un <see cref="decimal"/> que supera la precisión que admite Parquet.Net lanza una excepción
	///	al grabar, en lugar de truncarse en silencio
	/// </summary>
	[Fact]
	public async Task throw_when_a_decimal_exceeds_the_supported_precision()
	{
		using TempFile file = new();
		using FakeDataReader reader = FakeDataReader.CreateSingleColumn("Value", typeof(decimal), new object?[] { decimal.MaxValue });
		ParquetDataWriter writer = new(10);

			await Assert.ThrowsAsync<NotSupportedException>(() => writer.WriteAsync(file.FileName, reader, TestContext.Current.CancellationToken));
	}

	/// <summary>
	///		Comprueba que un <see cref="TimeSpan"/> sólo conserva precisión de milisegundos: los ticks por debajo del
	///	milisegundo se truncan (comportamiento del tipo lógico TIME de Parquet, comprobado experimentalmente)
	/// </summary>
	[Fact]
	public async Task truncate_time_span_ticks_below_millisecond_precision()
	{
		TimeSpan original = new TimeSpan(23, 59, 59).Add(TimeSpan.FromTicks(9_999_999));
		TimeSpan expected = new TimeSpan(0, 23, 59, 59, 999);

		using TempFile file = new();

			using (FakeDataReader reader = FakeDataReader.CreateSingleColumn("Value", typeof(TimeSpan), new object?[] { original }))
			{
				ParquetDataWriter writer = new(10);

					await writer.WriteAsync(file.FileName, reader, TestContext.Current.CancellationToken);
			}
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
				await reader.ReadAsync(TestContext.Current.CancellationToken);
				reader.GetValue(0).Should().Be(expected);
			}
	}

	/// <summary>
	///		Encadena un <see cref="ParquetDataReader"/> como origen de un <see cref="ParquetDataWriter"/>: comprueba
	///	que las dos mitades de la librería encajan entre sí y no sólo contra dobles de prueba a medida
	/// </summary>
	[Fact]
	public async Task chain_a_reader_into_a_writer()
	{
		using TempFile firstFile = new();
		using TempFile secondFile = new();

			// Escribe el primer archivo a partir de un origen de datos con varios tipos y un nulo
			using (FakeDataReader source = CreateSampleReader())
			{
				ParquetDataWriter writer = new(10);

					await writer.WriteAsync(firstFile.FileName, source, TestContext.Current.CancellationToken);
			}
			// Encadena: lee el primer archivo y lo vuelve a escribir en un segundo archivo
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(firstFile.FileName, TestContext.Current.CancellationToken);

				ParquetDataWriter writer = new(10);
				long written = await writer.WriteAsync(secondFile.FileName, reader, TestContext.Current.CancellationToken);

					written.Should().Be(2);
			}
			// Comprueba que el segundo archivo tiene los mismos datos que el primero
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(secondFile.FileName, TestContext.Current.CancellationToken);

					(await reader.ReadAsync(TestContext.Current.CancellationToken)).Should().BeTrue();
					reader["Name"].Should().Be("Foo");
					reader["Amount"].Should().Be(12.5m);
					reader["Active"].Should().Be(true);

					(await reader.ReadAsync(TestContext.Current.CancellationToken)).Should().BeTrue();
					reader.IsDBNull(reader.GetOrdinal("Name")).Should().BeTrue();
					reader["Amount"].Should().Be(-3.25m);
					reader["Active"].Should().Be(false);
			}
	}

	/// <summary>
	///		Comprueba que se puede escribir y leer un número de filas que no es múltiplo del tamaño del grupo de filas
	///	(dos grupos completos más un tercer grupo parcial)
	/// </summary>
	[Fact]
	public async Task write_and_read_rows_spanning_several_row_groups()
	{
		List<int> values = Enumerable.Range(0, 55).ToList();

		using TempFile file = new();

			using (FakeDataReader reader = FakeDataReader.CreateSingleColumn("Value", values))
			{
				ParquetDataWriter writer = new(20);
				long written = await writer.WriteAsync(file.FileName, reader, TestContext.Current.CancellationToken);

					written.Should().Be(55);
			}
			using (ParquetDataReader reader = new())
			{
				List<int> read = new();

					await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
					while (await reader.ReadAsync(TestContext.Current.CancellationToken))
						read.Add((int) reader.GetValue(0));
					read.Should().Equal(values);
			}
	}

	/// <summary>
	///		Crea un lector de varias columnas y varios tipos, con un nulo intercalado, para las pruebas de encadenado
	/// </summary>
	private static FakeDataReader CreateSampleReader()
	{
		(string Name, Type Type)[] columns =
			{
				("Id", typeof(Guid)),
				("Name", typeof(string)),
				("Date", typeof(DateTime)),
				("Amount", typeof(decimal)),
				("Active", typeof(bool))
			};
		List<object?[]> rows = new()
			{
				new object?[] { Guid.Parse("DC340CF2-331E-4B58-9F96-B5009EAA8987"), "Foo", new DateTime(2024, 1, 1), 12.5m, true },
				new object?[] { Guid.Parse("E649461F-F628-4D2B-B9C8-2CB7A5D07E0A"), null, new DateTime(2024, 1, 2), -3.25m, false }
			};

		return new FakeDataReader(columns, rows);
	}

	/// <summary>
	///		Compara un valor leído con el esperado, usando comparación de secuencia para matrices de bytes
	/// </summary>
	private static void AssertValueEquals(object? expected, object? actual)
	{
		if (expected is byte[] expectedBytes)
			((byte[]) actual!).Should().Equal(expectedBytes);
		else
			actual.Should().Be(expected);
	}
}
