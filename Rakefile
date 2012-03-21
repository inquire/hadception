require 'rake/clean'

# Build Metadata

NAME = 'hadception-0.92[eames]'
FRAMEWORK_NAME = 'nmr-eames'
DEPLOYMENT_NAME = 'inception'

# Configuration

BUILD_OPTIONS = "-d -Xlint -classpath"
COMPILE_OPTIONS = "-Xlint -classpath"

HADOOP_CORE = "$HADOOP_HOME/hadoop-core-0.20.204.0.jar"
HADOOP_CMD = "$HADOOP_HOME/lib/commons-cli-1.2.jar"

BUILD_DESTINATION = "buildbin"
BUILD_SOURCE= "framework/uk/ac/ed/inf/nmr/*/*.java"

COMPILE_DESTINATION = "inception"
USER_CODE = "jobs/*.java"

HADOOP_INPUT = "/tmp/nestin"
HADOOP_OUTPUT = "/tmp/outputs"

# Cleaning Files #

# list of locations on HDFS that you would like to clean
HDFS_CLEAN = ['/tmp/inceptions', '/tmp/outputs', '/tmp/nestout']

CLEAN.include(['framejar', 'inception', 'bin', 'buildbin', 'uk', 'META-INF'])
task :default => [:cleanHDFS, :cleanJAR, :build, :compile, :package, :deploy, :clean]

desc "Cleaning HDFS locations."
task :cleanHDFS do
  begin
    puts "Cleaning HDFS locations"
    HDFS_CLEAN.each do |cleaners|
      `hadoop fs -rmr #{cleaners}`
    end
  end
end

desc "Buildling the NMR framework."
task :build  do
  begin
    puts "\n>>>> Build release: #{NAME} <<<<\n\n"
  
    puts "Compile framework with provided information."
    `mkdir #{BUILD_DESTINATION}`
    `javac #{BUILD_OPTIONS} #{HADOOP_CORE}:#{HADOOP_CMD} -d #{BUILD_DESTINATION} #{BUILD_SOURCE}`
    
    puts "JAR the packages..."
    `jar cvf #{FRAMEWORK_NAME}.jar -C buildbin .`
  end
end

desc "Compile user code with NMR framework."
task :compile do
  begin
    puts "Compiling user code with default frameworks and NMR."
    `mkdir #{COMPILE_DESTINATION}` 
    `javac #{COMPILE_OPTIONS} #{HADOOP_CORE}:#{HADOOP_CMD}:#{FRAMEWORK_NAME}.jar -d #{COMPILE_DESTINATION} #{USER_CODE}`
    #{}`javac -Xlint -classpath $HADOOP_HOME/hadoop-core-0.20.204.0.jar:$HADOOP_HOME/lib/commons-cli-1.2.jar:./nmr-eames.jar -d inception src/*.java`
  end
end

desc "Package code for cluster."
task :package do
  begin
    puts "Creating a jar with user code and the NMR framework."
    `cp #{FRAMEWORK_NAME}.jar #{COMPILE_DESTINATION}`
    `cd #{COMPILE_DESTINATION} ; jar xvf #{FRAMEWORK_NAME}.jar ; cd ../`
    `jar cvf #{DEPLOYMENT_NAME}.jar -C #{COMPILE_DESTINATION} .`
  end
end

desc "Deploy to cluster with default settings."
task :deploy do
  begin
    'Starting cluster Job'
    `hadoop jar #{DEPLOYMENT_NAME}.jar MRMain -libjars #{DEPLOYMENT_NAME}.jar #{HADOOP_INPUT} #{HADOOP_OUTPUT}`
  end
end

desc "Clear user code & NMR JARs."
task :cleanJAR do
  begin
  `rm #{DEPLOYMENT_NAME}.jar`
  `rm #{FRAMEWORK_NAME}.jar`
  end
end


