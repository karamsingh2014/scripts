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
my $hadoop_heap_size = "2560";
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
    print " --hadoop-heap-size=<integer value of java heap size for hadoop in mb e.g --hadoop-heap-size=1536 for 1.5g default:2560 2.5g>\n";
    exit $exit_status;
}
GetOptions ("trace-path|t=s" => \$trace_path, "num-runs|n=i" => \$num_runs, "queue|q=s" => \$queue, "framework-name|f=s" => \$framework, "submit-resolver|s=s" => \$user_resolver,
            "users-file|u=s" => \$user_file, "submit-policy|p=s" => \$policy, "job-type|j=s" => \$job_type, "submit-threads|r=i" => \$submit_threads, 
            "submit-multiplier|m=f" => \$submit_multiplier, "max-sleep|e=i" => $max_sleep, "pending-queue-dept|d=i" => \$pending_queue_depth, "input-data-size|a=s" => \$input_data,
            "output-path|o=s" => \$output_path, "hadoop-heap-size=i" => \$hadoop_heap_size, "help|h" => sub {help_msg(0)}) or die("Error in command line arguments\n");
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
my $hadoop_classpath = "/usr/hdp/current/hadoop-mapreduce-client/hadoop-rumen.jar:/usr/hdp/current/hadoop-mapreduce-client/hadoop-gridmix.jar";
$hadoop_classpath .= ":$ENV{'HADOOP_CLASSPATH'}" if ($ENV{'HADOOP_CLASSPATH'});
$ENV{'HADOOP_CLASSPATH'} = $hadoop_classpath;
$ENV{'HADOOP_HEAPSIZE'} = $hadoop_heap_size;
$ENV{'HADOOP_CLIENT_OPTS'} = "-Xmx${hadoop_heap_size}m";
my $common_cmd = "hadoop org.apache.hadoop.mapred.gridmix.Gridmix -libjars \"/usr/hdp/current/hadoop-mapreduce-client/hadoop-rumen.jar,/usr/hdp/current/hadoop-mapreduce-client/hadoop-gridmix.jar\" \"-Dgridmix.min.file.size=0\" \"-Dgridmix.client.pending.queue.depth=$pending_queue_depth\" \"-Dgridmix.job-submission.policy=$policy\" \"-Dgridmix.client.submit.threads=$submit_threads\" \"-Dgridmix.submit.multiplier=$submit_multiplier\" \"-Dgridmix.job.type=$job_type\" \"-Dmapreduce.framework.name=$framework\" \"-Dtez.queue.name=$queue\" \"-Dmapreduce.job.queuename=$queue\" \"-Dmapred.job.queue.name=$queue\" \"-Dipc.client.connect.max.retries=10\" \"-Dgridmix.sleep.max-map-time=$max_sleep\" \"-Dgridmix.sleep.max-reduce-time=$max_sleep\"";

for(my $i=0; $i < $num_runs; $i++){
    my $outfile = "$output_path/gm_${queue}_$i.out";
    my $gm_root_dir = "/tmp/$luser/gmv3_$i";
    my $work_dir = "$gm_root_dir/work";
    my $output_dir = "$gm_root_dir/out";
    my $cmd = "$common_cmd \"-Dgridmix.output.directory=$output_dir\" ";
    system("hadoop fs -rm -r -skipTrash $gm_root_dir");
    if ($user_resolver =~ /submit/i){
        $cmd .= " \"-Dgridmix.user.resolve.class=org.apache.hadoop.mapred.gridmix.SubmitterUserResolver\"-generate $input_data $work_dir file://$trace_path >$outfile 2>&1";
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
    system("hadoop fs -rm -r -skipTrash $gm_root_dir");  
}
