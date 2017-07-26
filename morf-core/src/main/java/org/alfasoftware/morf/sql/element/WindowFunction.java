/* Copyright 2017 Alfa Financial Software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.alfasoftware.morf.sql.element;

import static org.alfasoftware.morf.util.DeepCopyTransformations.transformIterable;

import java.util.List;

import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Encapsulates the generation of an PARTITION BY SQL statement. <blockquote>
 *
 * <pre>
 *   SqlUtils.windowFunction([function])                         = [function]
 *        |----&gt; .partitionBy([fields]...)                    = [function] OVER (PARTITION BY [fields])
 *                |----&gt; .orderBy([fields]...)                = [function] OVER (PARTITION BY [fields] ORDER BY [fields])
 *        |----&gt; .orderBy([fields]...)                        = [function] OVER (ORDER BY [fields])
 * </pre>
 *
 * </blockquote> Restrictions:
 * <ul>
 * <li>partitionBy(..) is optional: If not specified it treats all the rows of
 * the result set as a single group.</li>
 * <li>orderBy(..) is optional. If not specified the entire partition will be
 * used as the window frame. If specified a range between the first row and the
 * current row of the window is used (i.e. RANGE UNBOUNDED PRECEDING AND CURRENT
 * ROW for Oracle).</li>
 * <li>The default direction for fields in orderBy(..) is ASC.</li>
 * </ul>
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public final class WindowFunction extends AliasedField implements Driver {

  private final Function                    function;
  private final ImmutableList<AliasedField> orderBys;
  private final ImmutableList<AliasedField> partitionBys;

  private WindowFunction(String alias,Function function, ImmutableList<AliasedField> orderBys, ImmutableList<AliasedField> partitionBy) {
    super(alias);
    this.function = function;
    this.orderBys = orderBys;
    this.partitionBys = partitionBy;
  }


  /**
   * Starts a new window function Builder.
   * @param function the function to construct the window function over.
   * @return the window function builder
   */
  public static Builder over(Function function) {
    return new BuilderImpl(function);
  }


  /**
   * @return the function.
   */
  public Function getFunction() {
    return function;
  }


  /**
   * @return the fields to order by.
   */
  public ImmutableList<AliasedField> getOrderBys() {
    return orderBys;
  }


  /**
   * @return the fields to partition by.
   */
  public ImmutableList<AliasedField> getPartitionBys() {
    return partitionBys;
  }


  /**
   * The complete window function Builder.
   *
   * @author Copyright (c) Alfa Financial Software 2017
   */
  public interface Builder extends AliasedFieldBuilder {

    /**
     * Specifies the fields to include in the ORDER BY clause of the window
     * function. If the fields do not contain an order direction, ASC will be
     * used.
     *
     * @param orderByFields the fields to order by.
     * @return The window function builder
     */
    Builder orderBy(AliasedField... orderByFields);


    /**
     * Specifies the fields to include in the ORDER BY clause of the window
     * function. If the fields do not contain an order direction, ASC will be
     * used.
     *
     * @param orderByFields the fields to order by.
     * @return the window function builder
     */
    Builder orderBy(Iterable<? extends AliasedField> orderByFields);


    /**
     * Specifies the fields to partition by.
     *
     * @param partitionByFields the fields to partition by.
     * @return the window function builder
     */
    Builder partitionBy(AliasedField... partitionByFields);


    /**
     * Specifies the fields to partition by.
     *
     * @param partitionByFields the fields to partition by.
     * @return the window function builder
     */
    Builder partitionBy(Iterable<? extends AliasedField> partitionByFields);
  }


  /**
   * Implementation of the window function Builder.
   *
   * @author Copyright (c) Alfa Financial Software 2017
   */
  private static final class BuilderImpl implements Builder {

    private String alias;
    private final Function           function;
    private final List<AliasedField> orderBys    = Lists.newArrayList();
    private final List<AliasedField> partitionBy = Lists.newArrayList();


    private BuilderImpl(Function function) {
      if (!(function.getType() == FunctionType.AVERAGE
          || function.getType() == FunctionType.SUM
          || function.getType() == FunctionType.COUNT
          || function.getType() == FunctionType.MIN
          || function.getType() == FunctionType.MAX)) {
        throw new IllegalArgumentException("Function of type [" + function.getType() + "] is not supported");
      }
      this.function = function;
    }


    @Override
    public Builder partitionBy(AliasedField... partitionByFields) {
      if(partitionByFields == null || partitionByFields.length == 0) {
        throw new IllegalArgumentException("No partitionBy fields specified");
      }

      this.partitionBy.addAll(FluentIterable.from(partitionByFields).toList());
      return this;
    }


    @Override
    public Builder partitionBy(Iterable<? extends AliasedField> partitionByFields) {
      this.partitionBy.addAll(FluentIterable.from(partitionByFields).toList());
      return this;
    }


    @Override
    public Builder orderBy(AliasedField... orderByFields) {
      if(orderByFields == null || orderByFields.length == 0) {
        throw new IllegalArgumentException("No orderBy fields specified");
      }

      this.orderBys.addAll(FluentIterable.from(orderByFields).toList());
      return this;
    }


    @Override
    public Builder orderBy(Iterable<? extends AliasedField> orderByFields) {
      this.orderBys.addAll(FluentIterable.from(orderByFields).toList());
      return this;
    }


    @Override
    public AliasedField build() {
      setOrderByAscendingIfUnset();
      return new WindowFunction(alias,function, ImmutableList.copyOf(orderBys), ImmutableList.copyOf(partitionBy));
    }


    private void setOrderByAscendingIfUnset() {
      for (AliasedField currentField : orderBys) {
        if (currentField instanceof FieldReference && ((FieldReference) currentField).getDirection() == Direction.NONE) {
          ((FieldReference) currentField).setDirection(Direction.ASCENDING);
        }
      }
    }


    /**
     * @see org.alfasoftware.morf.sql.element.AliasedFieldBuilder#as(java.lang.String)
     */
    @Override
    public AliasedFieldBuilder as(String alias) {
      this.alias = alias;
      return null;
    }
  }


  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#deepCopyInternal(DeepCopyTransformation)
   */
  @Override
  protected AliasedField deepCopyInternal(DeepCopyTransformation transformer) {
    return new WindowFunction(
      getAlias(),
      (Function) transformer.deepCopy(function),
      transformIterable(orderBys, transformer),
      transformIterable(partitionBys, transformer));
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser
      .dispatch(getFunction())
      .dispatch(getOrderBys())
      .dispatch(getPartitionBys());
  }
}