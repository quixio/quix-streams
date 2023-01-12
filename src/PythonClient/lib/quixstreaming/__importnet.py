# noinspection PyUnresolvedReferences
import clr
# noinspection PyUnresolvedReferences
import System.Reflection
import sys
import os
import platform
import xml.etree.ElementTree as ET
import zipfile

is_64bits = sys.maxsize > 2 ** 32
arch = "x64" if is_64bits else "x86"
os_name = platform.system()
dotnet_folder = os.path.join(os.path.dirname(__file__), "dotnet")
if os_name == "Windows":
    dotnet_folder = os.path.join(dotnet_folder, 'win-' + arch)
elif os_name == "Darwin":
    dotnet_folder = os.path.join(dotnet_folder, 'osx-' + arch)
elif os_name == "Linux":
    dotnet_folder = os.path.join(dotnet_folder, 'linux-' + arch)
if not os.path.isdir(dotnet_folder):
    raise NotImplementedError(os_name + " " + arch + " is not supported currently")

sys.path.append(dotnet_folder)

zipFilePath = os.path.join(dotnet_folder, "Quix.Sdk.Streaming.zip")
if os.path.exists(zipFilePath):
    with zipfile.ZipFile(zipFilePath, 'r') as zip_ref:
        zip_ref.extractall(dotnet_folder)
    os.remove(zipFilePath)

redirectFile = os.path.join(dotnet_folder, "Quix.Sdk.Streaming.dll.config")
if os.path.exists(redirectFile):
    tree = ET.parse(redirectFile)
    redirects = {}
    assembly_redirects = tree.findall('.//{urn:schemas-microsoft-com:asm.v1}dependentAssembly')
    for node in assembly_redirects:
        node_identity = node.find('{urn:schemas-microsoft-com:asm.v1}assemblyIdentity')
        node_redirect = node.find('{urn:schemas-microsoft-com:asm.v1}bindingRedirect')
        assembly_name = node_identity.attrib['name']
        redirects[assembly_name] = {'identity': node_identity, 'redirect': node_redirect}

    def on_assembly_resolve(sender, arg):
        requested_assembly = System.Reflection.AssemblyName(arg.Name)
        redirect_info = redirects.get(requested_assembly.Name)

        if redirect_info is None:
            return None

        expected_version = System.Version.Parse(redirect_info['redirect'].attrib['newVersion']);
        expected_public_key = System.Reflection.AssemblyName("x, PublicKeyToken=" + redirect_info['identity'].attrib['publicKeyToken']).GetPublicKeyToken()

        #print("Loading " + requested_assembly.Name)
        dll_path = os.path.join(dotnet_folder, requested_assembly.Name + ".dll")
        if System.IO.File.Exists(dll_path):
            assembly = System.Reflection.Assembly.LoadFrom(dll_path)
            assembly_name = System.Reflection.AssemblyName(assembly.FullName)
            if assembly_name.Version != expected_version:
                return None
            for index, byte in enumerate(assembly_name.GetPublicKeyToken()):
                if byte != expected_public_key[index]:
                    return None
            return assembly
        else:
            print("did not find " + dll_path)

        return None
    System.AppDomain.CurrentDomain.AssemblyResolve += on_assembly_resolve

clr.AddReference("Quix.Sdk.Streaming")
clr.AddReference("Quix.Sdk.Logging")
