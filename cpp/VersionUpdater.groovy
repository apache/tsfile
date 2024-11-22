/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Synchronize the version in CMakeLists.txt and the one used in the maven pom.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

def cppProjectFile = new File(project.basedir, "CMakeLists.txt")
def currentMavenVersion = project.version as String
def currentCppVersion = currentMavenVersion
if(currentMavenVersion.contains("-SNAPSHOT")) {
    currentCppVersion = currentMavenVersion.split("-SNAPSHOT")[0] + ".dev"
}
println "Current Project Version in Maven:  " + currentMavenVersion

def match = cppProjectFile.text =~ /set\(TsFile_CPP_VERSION\s+(.+?)\)/
def cppProjectFileVersion = match[0][1]
println "Current Project Version in CMake: " + cppProjectFileVersion

if (cppProjectFileVersion != currentCppVersion) {
    cppProjectFile.text = cppProjectFile.text.replace("set(TsFile_CPP_VERSION " + cppProjectFileVersion + ")", "set(TsFile_CPP_VERSION " + currentCppVersion + ")")
    println "Version in CMakeLists.txt updated from " + cppProjectFileVersion + " to " + currentCppVersion
    // TODO: When releasing, we might need to manually add this file to the release preparation commit.
} else {
    println "Version in CMakeLists.txt is up to date"
}
