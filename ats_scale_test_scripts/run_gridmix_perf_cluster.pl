use strict;
use warnings;
use Getopt::Long;
use Cwd 'abs_path';
my $output_path = "./";
my $num_runs = 1;
my $queue = "default";
my $framework = "yarn-tez";
my $user_resolver = "submit";
my $user_file = undef;
my $submit_multiplier = 0.0001;
my $submit_threads = 5;
my $pending_queue_depth = 10;
my $job_type = 'SLEEPJOB'; 
my $policy = 'REPLAY';
my $max_sleep = 500;
my $luser = getpwuid($<) || 'ksingh';
my $input_data = '1g';
my $trace_path = undef;
my $java_home = '/grid/4/home/rajesh/jdk1.8.0_60';
my $hadoop_home = $ENV{'HADOOP_HOME'} || '/grid/4/hadoop';
my $hdfs_home = $ENV{'HADOOP_HDFS_HOME'} || 'grid/4/hadoop-hdfs';
my $mr_home = $ENV{'HADOOP_MAPRED_HOME'} || '/grid/4/hadoop-mapreduce';
my $yarn_home = $ENV{'HADOOP_YARN_HOME'} || '/grid/4/hadoop-yarn';
my $tez_home = $ENV{'TEZ_HOME'} = '/grid/4/tez';
my $tools_home = "$hadoop_home/share/hadoop/tools"; 
my $hadoop_version = undef;
my $hadoop_conf = $ENV{'HADOOP_CONF_DIR'} || '/grid/4/home/ksingh/folded_mg_trace/hadoop-conf1';
my $tez_conf = $ENV{'TEZ_CONF_DIR'} || '/grid/4/home/ksingh/tez-conf';
my $hadoop_libexec_dir = $ENV{'HADOOP_LIBEXEC_DIR'} || "$hadoop_home/libexec";
sub help_msg{
    my $exit_status = shift @_ || 0;
    print "Usage $0: $0 <Options>\n\t -t | --trace-path=<[REQUIRED] GridMix rumen trace absolute path>\n\t -n | --num-runs=<Number of GridMix runs, default 1>\n\t";
    print " -o | --output-path <Output directory to output gm log, default: ./>\n\t -a | --input-data-size=<-generate <size>/ Input data genetation size, default 1g == 1GB>\n\t";
    print " -q | --queue=>yarn-queue, default: default queue>\n\t -f | --framework-name=<yarn frameowrk name, default: yarn-tez, values can be yarn, yarn-tez>\n\t";
    print " -s | --submit-resolver=<gridmix.user.resolve.class, accepted values are echo,submit,roundrobin, default submit>\n\t";
    print " -u | --users-file=<users file/<users-list>, absolute path of users file/file container users mapping, Required if -s/--submit-resolver=roundrobin>\n\t";
    print " -p | --submit-policy=<GridMix Submission Policy, Accepted values REPLAY/SERIAL/STRESS, default: REPLAY>\n\t";
    print " -j | --job-type=<GridMix job types, Accepted values LOADJOB/SLEEPJOB, default: SLEEPJOB>\n\t";
    print " -r | --submit-threads=<GridMix Client submit threads, default: 5>\n\t -d | --pending-queue-depth=<GridMix pending queue depth, default: 10>\n\t";
    print " -m | --submit-multiplier=<gridmix.submit.multiplier:default: 0.0001. The multiplier to accelerate/decelerate the submission. The time separating two jobs multiplier factor>\n\t";
    print " -e | --max-sleep=<-Dgridmix.sleep.max-map-time/-Dgridmix.sleep.max-reduce-time in milliseconds: default 500ms>\n\t";
    print " --hadoop-home=<hadoop home/hadoop common home path>\n\t --hdfs-home=<hadoop hdfs home path>\n\t --mr-home=<hadoop mapreduce home>\n\t --yarn-home=<hadoop yarn home>\n\t";
    print " --hadoop-conf=<hadoop conf dir>\n\t --java-home=<java home>\n\t --tez-home=< tez home path>\n\t --tez-conf=<tez conf dir>\n\t --hadoop-version=<hadoop version string>\n";
    exit $exit_status;
}
GetOptions ("trace-path|t=s" => \$trace_path, "num-runs|n=i" => \$num_runs, "queue|q=s" => \$queue, "framework-name|f=s" => \$framework, "submit-resolver|s=s" => \$user_resolver,
            "users-file|u=s" => \$user_file, "submit-policy|p=s" => \$policy, "job-type|j=s" => \$job_type, "submit-threads|r=i" => \$submit_threads, 
            "submit-multiplier|m=f" => \$submit_multiplier, "max-sleep|e=i" => $max_sleep, "pending-queue-dept|d=i" => \$pending_queue_depth, "input-data-size|a=s" => \$input_data,
            "output-path|o=s" => \$output_path, "hadoop-home=s" => \$hadoop_home, "hdfs-home=s" => \$hdfs_home, "mr-home=s" =>\$mr_home, "yarn-home=s" => \$yarn_home,
            "java-home=s" => \$java_home, "hadoop-conf=s" => \$hadoop_conf, "tez-home=s" =>\$tez_home, "tez-conf=s" => \$tez_conf, "hadoop-version=s" => \$hadoop_version,
            "help|h" => sub {help_msg(0)}) or die("Error in command line arguments\n");
unless($trace_path && -f $trace_path && -r $trace_path ){
    print STDERR "ERROR: Invalid trace-path specified. Not specified/Not file/Not readable\n";
    help_msg(1);
}
$trace_path = abs_path($trace_path);
unless ($trace_path =~ /^\//){
    print STDERR "ERROR: $trace_path is not absolute\n";
    help_msg(1);
}
unless ($output_path && -d $output_path && -w $output_path && -x $output_path){
    print STDERR "ERROR: Invalid output-path specified. Not Specified/Not directory/Not Writable/Not Executable\n";
    help_msg(1);
}

unless($framework eq 'yarn' || $framework eq 'yarn-tez'){
    print STDERR "WARN: Invalid mr framework name specified. Accepted values are 'yarn/yarn-tez'. Using default value yarn-tez\n";
    $framework = 'yarn-tez';
}

unless($job_type eq 'SLEEPJOB' || $job_type eq 'LOADJOB'){
    print STDERR "WARN: Invalid job-type specified. Accepted values are 'SLEEPJOB/LOADJOB'. Using default value SLEEPJOB\n";
    $job_type = "SLEEPJOB";
}

unless($policy eq 'REPLAY' || $policy eq 'SERIAL' || $policy eq 'STRESS'){
    print STDERR "WARN: Invalid submission policy specified. Accepted values are 'REPLAY/SERIAL/STRESS'. Using default value: REPLAY\n";
    $policy = "REPLAY"
}

unless($user_resolver =~ /submit/i || $user_resolver =~ /echo/i || $user_resolver =~ /roundrobin/i){
    print STDERR "WARN: Invalied submit user resolver sepcied. Accpeted values are 'submit/echo/roundrobin'. Using default value: submit\n";
    $user_resolver="submit"
}
if ($user_resolver =~ /roundrobin/i){
    $user_file = abs_path($user_file) if ($user_file);
    unless ($user_file && -f $user_file && -r $user_file && $user_file =~ /^\//){
        print STDERR "ERROR: Invalid users files specified . Not specified/Not file/Not readable/Not absolute\n";
        help_msg(1)
    }
}
my $hadoop_classpath = "$hadoop_conf:$hadoop_home/*:$hadoop_home/lib/*:$hdfs_home/*:$hdfs_home/lib/*:$mr_home/*:$mr_home/lib/*:$yarn_home/*:$yarn_home/lib/*";
if ($framework eq 'yarn-tez'){
    $hadoop_classpath = "$tez_conf:$tez_home/*:$tez_home/lib/*";
}
my $rumen_jar = "$mr_home/hadoop-rumen.jar";
my $gridmix_jar="$mr_home/hadoop-gridmix.jar";
if ( !-e $rumen_jar){
    if (-e "$tools_home/lib/hadoop-rumen.jar"){
        $rumen_jar = "$tools_home/lib/hadoop-rumen.jar";
    }elsif ( $hadoop_version && -e "$tools_home/lib/hadoop-rumen-$hadoop_version.jar" ){
        $rumen_jar = "$tools_home/lib/hadoop-rumen-$hadoop_version.jar";
    }elsif( -e "$tools_home/hadoop-rumen.jar" ){
        $gridmix_jar = "$tools_home/hadoop-rumen.jar";
    }elsif ($hadoop_version && -e "$tools_home/hadoop-rumen-$hadoop_version.jar" ){
        $gridmix_jar = "$tools_home/hadoop-rumen-$hadoop_version.jar";
    }
}
die "hadoop rumen jar not found in $mr_home $tools_home/ $tools_home/lib" unless (-e $rumen_jar);
if ( !-e $gridmix_jar){
    if (-e "$tools_home/lib/hadoop-gridmix.jar" ){
        $gridmix_jar = "$tools_home/lib/hadoop-gridmix.jar";
    }elsif ( $hadoop_version && -e "$tools_home/lib/hadoop-gridmix-$hadoop_version.jar" ){
        $gridmix_jar = "$tools_home/lib/hadoop-gridmix-$hadoop_version.jar";
    }elsif( -e "$tools_home/hadoop-gridmix.jar" ){
        $gridmix_jar = "$tools_home/hadoop-gridmix.jar";
    }elsif ($hadoop_version && -e "$tools_home/hadoop-gridmix-$hadoop_version.jar" ){
        $gridmix_jar = "$tools_home/hadoop-gridmix-$hadoop_version.jar";
    }
}
die "hadoop gridmix jar not found in $mr_home $tools_home/ $tools_home/lib/" unless( -e $gridmix_jar);
$hadoop_classpath .= ":$rumen_jar:$gridmix_jar";
$hadoop_classpath .= ":$ENV{'HADOOP_CLASSPATH'}" if ($ENV{'HADOOP_CLASSPATH'});
$ENV{'HADOOP_CLASSPATH'} = $hadoop_classpath;
$ENV{'HADOOP_HOME'} = $hadoop_home;
$ENV{'HADOOP_COMMON_HOME'} = $hadoop_home;
$ENV{'HADOOP_HDFS_HOME'} = $hdfs_home;
$ENV{'HADOOP_MAPRED_HOME'} = $mr_home;
$ENV{'HADOOP_YARN_HOME'} = $yarn_home;
$ENV{'HADOOP_CONF_DIR'} = $hadoop_conf;
$ENV{'HADOOP_LIBEXEC_DIR'} = $hadoop_libexec_dir;
$ENV{'TEZ_HOME'} = $tez_home;
$ENV{'TEZ_CONF_DIR'} = $tez_conf;
my $common_cmd = "$hadoop_home/bin/hadoop org.apache.hadoop.mapred.gridmix.Gridmix -libjars \"$rumen_jar,$gridmix_jar\" \"-Dgridmix.min.file.size=0\" \"-Dgridmix.client.pending.queue.depth=$pending_queue_depth\" \"-Dgridmix.job-submission.policy=$policy\" \"-Dgridmix.client.submit.threads=$submit_threads\" \"-Dgridmix.submit.multiplier=$submit_multiplier\" \"-Dgridmix.job.type=$job_type\" \"-Dmapreduce.framework.name=$framework\" \"-Dtez.queue.name=$queue\" \"-Dmapreduce.job.queuename=$queue\" \"-Dmapred.job.queue.name=$queue\" \"-Dipc.client.connect.max.retries=10\" \"-Dgridmix.sleep.max-map-time=$max_sleep\" \"-Dgridmix.sleep.max-reduce-time=$max_sleep\"";

for(my $i=0; $i < $num_runs; $i++){
    my $outfile = "$output_path/gm_${queue}_$i.out";
    my $gm_root_dir = "/tmp/$luser/gmv3_$i";
    my $work_dir = "$gm_root_dir/work";
    my $output_dir = "$gm_root_dir/out";
    my $cmd = "$common_cmd \"-Dgridmix.output.directory=$output_dir\" ";
    #print $user_resolver," $i\n";
    if ($user_resolver =~ /submit/i){
        $cmd .= " \"-Dgridmix.user.resolve.class=org.apache.hadoop.mapred.gridmix.SubmitterUserResolver\" -generate $input_data $work_dir file://$trace_path >$outfile 2>&1";
        system($cmd);
    }
    if ($user_resolver =~ /echo/i){
        $cmd .= " \"-Dgridmix.user.resolve.class=org.apache.hadoop.mapred.gridmix.EchoUserResolver\" -generate $input_data $work_dir file://$trace_path >$outfile 2>&1";
        system($cmd);
    }
    if ($user_resolver =~ /roundrobin/i){
        $cmd .= " \"-Dgridmix.user.resolve.class=org.apache.hadoop.mapred.gridmix.RoundRobinUserResolver\" -generate $input_data -users file://$user_file ";
        $cmd .= " $work_dir file://$trace_path >$outfile 2>&1";
        system($cmd);
    }
    #system($cmd);
    system("$hadoop_home/bin/hadoop fs -rm -r -skipTrash $gm_root_dir");  
}
