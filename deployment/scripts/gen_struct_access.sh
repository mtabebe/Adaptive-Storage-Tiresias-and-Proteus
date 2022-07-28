#/bin/bash
# sample usage
# ./deployment/scripts/gen_struct_access.sh code/src/benchmark/tpcc/record-types/input code/src/benchmark/tpcc/record-types/output global deployment/scripts/gen_struct_accesses.py


inputDir="$1"
outputDir="$2"
globalOutLabel="$3"
scriptFile="$4"

mkdir -p $outputDir

globalOutDefFile="$outputDir/$globalOutLabel-def"
globalOutImplFile="$outputDir/$globalOutLabel-impl"
globalOutStructFile="$outputDir/$globalOutLabel-struct"
globalOutTypeFile="$outputDir/$globalOutLabel-type"
globalOutCDefFile="$outputDir/$globalOutLabel-const-def"
globalOutCImplFile="$outputDir/$globalOutLabel-const-impl"
globalOutJavaFile="$outputDir/$globalOutLabel-java"

rm $globalOutDefFile
rm $globalOutImplFile
rm $globalOutStructFile
rm $globalOutTypeFile
rm $globalOutCDefFile
rm $globalOutCImplFile
rm $globalOutJavaFile

for file in $inputDir/*
do
    echo "$file"
    name=$( echo $file | rev | cut -d "/" -f 1 | rev )
    echo $name

    inputFile=$file
    outputDefFile=$outputDir/$name-def
    outputImplFile=$outputDir/$name-impl
    outputStructFile=$outputDir/$name-struct
    outputTypeFile=$outputDir/$name-types
    outputCDefFile=$outputDir/$name-const-def
    outputCImplFile=$outputDir/$name-const-impl
    outputJavaFile=$outputDir/$name-java

    python3 $scriptFile $inputFile $outputDefFile $outputImplFile $outputStructFile $outputTypeFile $outputCDefFile $outputCImplFile $outputJavaFile

    cat $outputDefFile >> $globalOutDefFile
    cat $outputImplFile >> $globalOutImplFile
    cat $outputStructFile >> $globalOutStructFile
    cat $outputTypeFile >> $globalOutTypeFile
    cat $outputCDefFile >> $globalOutCDefFile
    cat $outputCImplFile >> $globalOutCImplFile
    cat $outputJavaFile >> $globalOutJavaFile
done
