package com.alpine.templates.hadoop.modelPluginTemplate

import com.alpine.model.RegressionRowModel
import com.alpine.model.export.pfa.{PFAConverter, PFAConvertible}
import com.alpine.model.pack.ml.sql.LinearRegressionSQLTransformer
import com.alpine.model.pack.util.TransformerUtil
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.RegressionTransformer
import com.alpine.util.FilteredSeq

// TODO: The model here essentially shows a linear regression model, replace the inputs and outputs as required
//  For the model export to be successful the SDK expects the user to supply the fully qualified name of the model
//  class in an exports.xml file provisioned under resources folder of the package. An example is provided under
//  resources folder which needs to be changed accordingly
/**
 * Representation of the classical linear regression model
 * y = intercept + a_0 x_0 + a_1 x_1 + ... + a_n x_n.
 *
 * The length of the coefficients must match the length of the inputFeatures.
 *
 * We use java.lang.Double for the type of the coefficients, because the scala Double type information
 * is lost by scala/Gson and the deserialization fails badly for edge cases (e.g. Double.NaN).
 *
 * @param coefficients         Vector of coefficients of the model. Must match the length of inputFeatures.
 * @param inputFeatures        Description of the (numeric) input features. Must match the length of coefficients.
 * @param intercept            Intercept of the model (defaults to 0).
 * @param dependentFeatureName Name used to identify the dependent feature in an evaluation dataset.
 * @param identifier           Used to identify this model when in a collection of models. Should be simple characters,
 *                             so it can be used in a feature name.
 */

case class ModelPluginTemplateModel(coefficients: Seq[java.lang.Double],
                                    inputFeatures: Seq[ColumnDef],
                                    intercept: Double = 0,
                                    dependentFeatureName: String = "",
                                    override val identifier: String = "")
  extends RegressionRowModel {

  override def transformer = new ModelPluginTemplateModelTransformer(coefficients, intercept)

  override def dependentFeature = ColumnDef(dependentFeatureName, ColumnType.Double)

  // override def sqlTransformer(sqlGenerator: SQLGenerator) = Some(new LinearRegressionSQLTransformer(this, sqlGenerator))
  // TODO: To add PFA generation extend PFAConvertible
  // override def getPFAConverter: PFAConverter = LinearRegressionPFAConverter(this)

  override def streamline(requiredOutputFeatureNames: Seq[String]): RegressionRowModel = {

    ModelPluginTemplateModel(
      this.coefficients,
      this.inputFeatures,
      this.intercept,
      this.dependentFeatureName,
      this.identifier);
  }

}

object ModelPluginTemplateModel {

  /**
   * An alternative constructor for LinearRegressionModel.
   *
   * Call this "make" instead of "apply" because the constructors need to be distinguishable after type erasure.
   *
   * @param coefficients         Vector of coefficients of the model. Must match the length of inputFeatures.
   * @param inputFeatures        Description of the (numeric) input features. Must match the length of coefficients.
   * @param intercept            Intercept of the model (defaults to 0).
   * @param dependentFeatureName Name used to identify the dependent feature in an evaluation dataset.
   * @param identifier           Used to identify this model when in a collection of models. Should be simple characters,
   *                             so it can be used in a feature name.
   */

  def make(coefficients: Seq[Double],
           inputFeatures: Seq[ColumnDef],
           intercept: Double = 0,
           dependentFeatureName: String = "", identifier: String = ""): ModelPluginTemplateModel = {

    ModelPluginTemplateModel(
      TransformerUtil.toJavaDoubleSeq(coefficients),
      inputFeatures,
      intercept,
      dependentFeatureName,
      identifier);
  }
}

class ModelPluginTemplateModelTransformer(coefficients: Seq[java.lang.Double],
                                          intercept: Double) extends RegressionTransformer
{
  // TODO: predict function here shows a linear regression type prediction, replace with your version
  override def predict(row: Row): Double = {
    require(row.length==coefficients.length, "Number of coefficients should be equal to number of features")
    // Calculate the predicted value
    // Try/Catch to detect underflow/overflow and return NaN
    try {
      coefficients.zip(row.map(v => v.asInstanceOf[Double])).foldLeft(intercept)((v,t) => v + t._1*t._2);
    }
    catch {
      case _: Throwable => Double.NaN
    }
  }
}

