package com.udf.trinoudf;

import io.trino.spi.Plugin;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;

import java.util.List;
import java.util.Set;

public class CustomUdfPlugin implements Plugin {

    @Override
    public Set<Class<?>> getFunctions() {
        return Set.of(CustomUdf.class);
    }

    @ScalarFunction("custom_udf")
    public static class CustomUdf {
        private final TypeManager typeManager;

        public CustomUdf(TypeManager typeManager) {
            this.typeManager = typeManager;
        }

        @SqlType("array(row(title1 varchar, title2 varchar))")
        public Block customUdf(
                @SqlType("array(row(title1 varchar, title2 varchar, title3 varchar))") Block inputArray) {
            Type rowType = typeManager.getType(new TypeSignature(
                    "row",
                    List.of(
                        TypeSignatureParameter.typeParameter(new TypeSignature("varchar")),
                        TypeSignatureParameter.typeParameter(new TypeSignature("varchar")),
                        TypeSignatureParameter.typeParameter(new TypeSignature("varchar"))
                    )
            ));
            List<Type> rowTypes = rowType.getTypeParameters();
            RowType outputRowType = RowType.from(List.of(
                RowType.field("title1", rowTypes.get(0)),
                RowType.field("title2", rowTypes.get(1))
            ));
            ArrayType arrayType = new ArrayType(outputRowType);

            BlockBuilder arrayBlockBuilder = arrayType.createBlockBuilder(null, inputArray.getPositionCount());

            for (int i = 0; i < inputArray.getPositionCount(); i++) {
                if (!inputArray.isNull(i)) {
                    Block rowBlock = (Block) rowType.getObject(inputArray, i);
                    BlockBuilder rowBlockBuilder = outputRowType.createBlockBuilder(null, 1);

                    rowTypes.get(0).appendTo(rowBlock, 0, rowBlockBuilder);
                    rowTypes.get(1).appendTo(rowBlock, 1, rowBlockBuilder);

                    arrayBlockBuilder.append(rowBlockBuilder.buildValueBlock(), 0);
                } else {
                    arrayBlockBuilder.appendNull();
                }
            }

            return arrayBlockBuilder.build();
        }
    }
}
