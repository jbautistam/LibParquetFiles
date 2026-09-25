using FluentAssertions;
using Bau.Libraries.LibParquetFiles.Writers.Models;

namespace ParquetFiles.Tests;

/// <summary>
///		Pruebas de <see cref="ParquetColumnModel"/> (clase interna, accesible gracias a InternalsVisibleTo)
/// </summary>
public class ParquetColumnModel_Should
{
	/// <summary>
	///		Tipo CLR (no anulable: así es como Parquet.Net expone <c>DataField.ClrType</c>) esperado para cada
	///	<see cref="ParquetColumnModel.FieldType"/>
	/// </summary>
	public static IEnumerable<object[]> FieldDataTypes()
	{
		yield return new object[] { ParquetColumnModel.FieldType.Boolean, typeof(bool) };
		yield return new object[] { ParquetColumnModel.FieldType.Byte, typeof(byte) };
		yield return new object[] { ParquetColumnModel.FieldType.DateTime, typeof(DateTime) };
		yield return new object[] { ParquetColumnModel.FieldType.Decimal, typeof(decimal) };
		yield return new object[] { ParquetColumnModel.FieldType.Double, typeof(double) };
		yield return new object[] { ParquetColumnModel.FieldType.Integer, typeof(int) };
		yield return new object[] { ParquetColumnModel.FieldType.Long, typeof(long) };
		yield return new object[] { ParquetColumnModel.FieldType.Guid, typeof(Guid) };
		yield return new object[] { ParquetColumnModel.FieldType.Time, typeof(TimeSpan) };
		yield return new object[] { ParquetColumnModel.FieldType.ByteArray, typeof(byte[]) };
		yield return new object[] { ParquetColumnModel.FieldType.String, typeof(string) };
	}

	/// <summary>
	///		Comprueba que cada tipo de columna genera un campo parquet anulable del tipo CLR esperado
	/// </summary>
	[Theory]
	[MemberData(nameof(FieldDataTypes))]
	public void create_a_parquet_field_of_the_expected_clr_type(object fieldType, Type expectedClrType)
	{
		ParquetColumnModel model = new((ParquetColumnModel.FieldType) fieldType, "Value", 10);

			model.ParquetField.ClrType.Should().Be(expectedClrType);
			model.ParquetField.IsNullable.Should().BeTrue();
	}

	/// <summary>
	///		Comprueba que se cuenta cada valor añadido, incluidos los nulos
	/// </summary>
	[Fact]
	public void count_added_values()
	{
		ParquetColumnModel model = new(ParquetColumnModel.FieldType.Integer, "Value", 10);

			model.AddInteger(1);
			model.AddInteger(2);
			model.AddNull();

			model.Count.Should().Be(3);
	}

	/// <summary>
	///		Comprueba que Clear reinicia el contador a cero
	/// </summary>
	[Fact]
	public void reset_count_on_clear()
	{
		ParquetColumnModel model = new(ParquetColumnModel.FieldType.Integer, "Value", 10);

			model.AddInteger(1);
			model.Clear();

			model.Count.Should().Be(0);
	}

	/// <summary>
	///		Comprueba que el tamaño máximo del grupo de filas debe ser positivo
	/// </summary>
	[Theory]
	[InlineData(0)]
	[InlineData(-1)]
	public void throw_when_max_values_is_not_positive(int maxValues)
	{
		Action act = () => new ParquetColumnModel(ParquetColumnModel.FieldType.Integer, "Value", maxValues);

			act.Should().Throw<ArgumentOutOfRangeException>();
	}

	/// <summary>
	///		Comprueba que no se puede sobrepasar el tamaño máximo del buffer (en lugar de lanzar un
	///	IndexOutOfRangeException al escribir directamente sobre el array)
	/// </summary>
	[Fact]
	public void throw_when_the_buffer_is_full()
	{
		ParquetColumnModel model = new(ParquetColumnModel.FieldType.Integer, "Value", 1);

			model.AddInteger(1);

			Action act = () => model.AddInteger(2);

			act.Should().Throw<InvalidOperationException>();
	}

	/// <summary>
	///		Comprueba que la comprobación de capacidad también se aplica al añadir un nulo
	/// </summary>
	[Fact]
	public void throw_when_the_buffer_is_full_adding_a_null()
	{
		ParquetColumnModel model = new(ParquetColumnModel.FieldType.String, "Value", 1);

			model.AddNull();

			Action act = () => model.AddNull();

			act.Should().Throw<InvalidOperationException>();
	}
}
