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
// Synchronize the version in setup.py and the one used in the maven pom.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

def pyProjectFile = new File(project.basedir, "setup.py")
def currentMavenVersion = project.version as String
def currentPyVersion = currentMavenVersion
if(currentMavenVersion.contains("-SNAPSHOT")) {
    currentPyVersion = currentMavenVersion.split("-SNAPSHOT")[0] + ".dev"
}
println "Current Project Version in Maven:  " + currentMavenVersion
def match = pyProjectFile.text =~ /version\s*=\s*"(.*?)"/
def pyProjectFileVersion = match[0][1]
println "Current Project Version in setup.py: " + pyProjectFileVersion

if (pyProjectFileVersion != currentPyVersion) {
    pyProjectFile.text = pyProjectFile.text.replace("version = \"" + pyProjectFileVersion + "\"", "version = \"" + currentPyVersion + "\"")
    println "Version in setup.py updated from " + pyProjectFileVersion + " to " + currentPyVersion
    // TODO: When releasing, we might need to manually add this file to the release preparation commit.
} else {
    println "Version in setup.py is up to date"
}
