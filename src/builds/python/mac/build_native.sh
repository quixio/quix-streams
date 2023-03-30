#!/bin/zsh

_=$(python3 -m pip show wheel)
if [[ $? != 0 ]]; then
	python3 -m pip install wheel
fi

_=$(dotnet --version)
if [[ $? != 0 ]]; then
	curl -L https://dot.net/v1/dotnet-install.sh -o dotnet-install.sh
	chmod +x ./dotnet-install.sh
    if [ $(arch) == "i386" ]; then
        ./dotnet-install.sh --channel 7.0.1xx
    else
        ./dotnet-install.sh --version 8.0.100-alpha.1.23055.13 --channel 8.0.1xx
    fi
	echo "export DOTNET_ROOT=$HOME/.dotnet" >> ~/.zshrc
	echo "export PATH=$PATH:$HOME/.dotnet:$HOME/.dotnet/tools" >> ~/.zshrc
	source ~/.zshrc
fi

archname=$(uname -m)
pythonplatform=$(python3 -c 'import platform;plat=platform.uname();print(f"{plat.system}-{plat.machine}".lower())')
# based on https://github.com/dotnet/runtime/blob/main/src/libraries/Microsoft.NETCore.Platforms/src/runtime.json
if [[ $archname == 'arm64' ]]; then
    dotnetruntime=osx.11.0-arm64
elif [[ $archname == 'x86_64' ]]; then
    dotnetruntime=osx-x64
else
  echo "Not yet supported architecture $archname"
  exit 1
fi

echo "Building for Mac architecture $archname with dotnet runtime id $dotnetruntime with python platform $pythonplatform"
sleep 1
interopfolder=../../../InteropGenerator
csharpfolder=../../../CsharpClient
pythonfolder=../../../PythonClient
streamingoutpath=$csharpfolder/QuixStreams.Streaming/bin/Publish/mac
echo build streaming
dotnet publish $csharpfolder/QuixStreams.Streaming/QuixStreams.Streaming.csproj -f net8.0 -c debug -o $streamingoutpath
if [[ $? != 0 ]]; then
	echo "Failed dotnet build QuixStreams.Streaming.csproj"
	exit 2
fi

interopgeneratoroutput=$interopfolder/Quix.InteropGenerator/bin/Publish/mac
echo build interop generator
dotnet publish $interopfolder/Quix.InteropGenerator/Quix.InteropGenerator.csproj -c release -o $interopgeneratoroutput
if [[ $? != 0 ]]; then
	echo "Failed dotnet publish Quix.InteropGenerator.csproj"
	exit 3
fi

echo run interop generator
interopoutput=$interopfolder/InteropOutput
interopconfig=$interopfolder/InteropConfig
"$interopgeneratoroutput/Quix.InteropGenerator" -a "$streamingoutpath/QuixStreams.Streaming.dll" -o "$interopoutput"  -c "$interopconfig"
if [[ $? != 0 ]]; then
	echo "Failed InteropGenerator run"
	exit 4
fi

dest=$pythonfolder/src/quixstreams/native
destPython=$dest/Python
destPlatform=$dest/$pythonplatform
echo Cleaning native folders...
rm -rf "$destPython"
rm -rf "$destPlatform"

echo Build interop
interopoutputcsharp=$interopoutput/Csharp
for assemblydir in $interopoutputcsharp/* ; do
    assemblyname=$(basename $assemblydir)
	echo dotnet publish $assemblydir/$assemblyname.csproj /p:NativeLib=Shared /p:SelfContained=true -r $dotnetruntime -c Debug -o $destPlatform/$assemblyname

    dotnet publish $assemblydir/$assemblyname.csproj /p:NativeLib=Shared /p:SelfContained=true -r $dotnetruntime -c Debug -o $destPlatform/$assemblyname
	if [[ $? != 0 ]]; then
		echo "Failed dotnet publish $assemblyname.csproj"
		exit 5
	fi
done

searchpaths=('/usr/local/Cellar' '/opt/homebrew/Cellar' '/usr/local/lib')

function copydylibdeps {
	dylib_filepath=$1
	dylib_filename=$(basename $dylib_filepath)
	dylib_file_folder=$(dirname $dylib_filepath)
	typeset -a files_to_inspect
	files_to_inspect=("$dylib_filepath")
	typeset -a file_names_to_inspect
	file_names_to_inspect=("$dylib_filename")

	typeset -a copied_dylibs
	copied_dylibs=()
	i=0
	while [[ $i -lt ${#files_to_inspect[@]} ]]; do
		i=$((i + 1))
		file_to_inspect=${files_to_inspect[$i]}	
		file=$(basename $file_to_inspect)
		echo "Handling $file"
		echo "\tOriginal location: $file_to_inspect"

		if [[ $file_to_inspect == /usr/lib/* ]]; then
			echo "\tSystem file, skipping"
			continue
		fi
		if [[ $file_to_inspect == /System/Library/* ]]; then
			echo "\tSystem file, skipping"
			continue
		fi

		file_new_dest=$dylib_file_folder/$file
		if [[ -f "$file_new_dest" ]]; then
			echo "\tFile already copied to necessary location"
			file_to_inspect=$file_new_dest
			copied_dylibs+=("$file_new_dest")
		else
			if [[ -f "$file_to_inspect" ]]; then
				echo "\tFound at original location"
			else
				echo "\tNeeds searching, didn't find at original location."
				found=false
				for searchpath in "${searchpaths[@]}"; do
					echo "\t\tSearching at $searchpath"
					searchoutput=$(find $searchpath -name $file)
					while read line; do
						if [[ -z "" ]]; then
							continue
						fi
						file_to_inspect=$line
						found=true
						echo "\t\t\t$file found at $file_to_inspect"
						break
					done <<< "$searchoutput"
					if [[ "$found" = "true" ]]; then
						break
					fi
				done
				if [[ "$found" = false ]]; then
					echo "\t\tDid not find anywhere"
					continue
				fi
			fi

			echo "\tCopying file to $file_new_dest"
			cp $file_to_inspect $file_new_dest
			if [[ $? != 0 ]]; then
				echo "\t!!! Failed to copy file"
				exit 6
			fi
			copied_dylibs+=("$file_new_dest")
			echo "\tCopy finished"
		fi

		output=$(otool -L $file_to_inspect | grep "dylib (" | cut -f 2 | cut -d " " -f 1)
		if [[ $? != 0 ]]; then
			echo "Failed to otool -L $file_to_inspect"
			echo $output
			exit 7
		fi
		while read line; do
			linefilename=$(basename $line)
			if [[ ${file_names_to_inspect[(ie)$linefilename]} -le ${#file_names_to_inspect} ]]; then
				continue
			fi
			files_to_inspect+=( "$line" )		
			file_names_to_inspect+=("$linefilename")
		done <<< "$output"
	done
}

function add_top_rpath {
	# Add relative runpath to each top level dylib otherwise might not load deps correctly
	dylib_filepath=$1
	dylib_filename=$(basename $dylib_filepath)
	dylib_file_folder=$(dirname $dylib_filepath)
	dylib_file_folder_name=$(basename $dylib_file_folder)
	dylib_filename_next=$(basename $dylib_filename .dylib)

	# if the folder name equals to the dylib name (without extension) then considered top level
	if [[ "$dylib_filename_next" == "$dylib_file_folder_name" ]]; then
		install_name_tool -add_rpath @loader_path $dylib_filepath
		if [[ $? != 0 ]]; then
			echo "Failed install_name_tool -add_rpath for $dylib_filepath"
			exit 8
		fi
		echo "Added @loader_path to $dylib_filepath"
	fi
}

dylibsoutput=$(find $destPlatform -name "*.dylib")
while read dylib_filepath; do
	copydylibdeps $dylib_filepath
	if [[ $? != 0 ]]; then
		echo "Failed copydylibdeps"
		exit 9
	fi
	add_top_rpath $dylib_filepath
	if [[ $? != 0 ]]; then
		echo "Failed add_top_rpath"
		exit 10
	fi
done <<< "$dylibsoutput"

echo Copy python interop to native
cp -R  "$interopoutput/Python/" "$destPython/"

