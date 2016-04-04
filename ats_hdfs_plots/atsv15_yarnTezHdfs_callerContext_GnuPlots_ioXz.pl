use strict;
use warnings;
use Date::Parse;
use File::Basename;
use Getopt::Long;
use Cwd 'abs_path';
use IO::Uncompress::UnXz;
my $input_pattrn = undef;
#exit 1 unless $input_path;
#my $default_period = 60;
#my $period = shift || undef;

my $default_period = 60;
my $period = undef; #$default_period; #shift || undef;
my $max_time = undef;
my $start_time = undef;
my $gnu_plot_home='/usr/local/bin/gnuplot';

sub help_msg{
    my $exit_status = shift @_ || 0;
    print "Usage $0: $0 <Options>\n\t -i | --input-glob-pattern=<[REQUIRED] Path of HDFS autdit logs with glob pattern e.g /tmp/hdfs-audit*.log*>\n\t";
    print " -p | --period=<[REQUIRED] Default timestamp/period difference between 2 scan intervals, default 60 secs>\n\t";
    print " -m | --max-time=<Max time in seconds to be considered to stop parsing>\n\t";
    print " -s | --start-time=<Start time to start parsing for plotting>\n\t";
    print " -g | --gnu-plot-path=<path of gnuplot binary, default is /usr/local/bin/gnuplot\n";

    exit $exit_status;
}
GetOptions ("input-glob-pattern|i=s" => \$input_pattrn, "max-time|m=i" => \$max_time, "start-time|s=i" => \$start_time, "period|p=i" => \$period, 
           "gnu-plot-path|g=s" => \$gnu_plot_home, "help|h" => sub { help_msg(0) }) or die("Error in command line arguments\n");

unless ($input_pattrn){
    print STDERR "ERROR: Invalied NameNode HDFS audit log dir path and pattern -i | --input-glob-pattern not provided\n";
    help_msg(1);
}
my $io_path = dirname($input_pattrn);
$io_path = abs_path($io_path);
unless (-d dirname($io_path) && -r dirname($io_path) && -x dirname($io_path)) {
    print STDERR "ERROR: Invalied NameNode HDFS audit log dir path of pattern. Not Directory/Readable/Writable/Executable (not having drwx)\n";
    help_msg(1);
}
$period = undef if($period && $period !~ /^\d+/);
$period = undef if(defined($period) && ($period % $default_period) != 0);
#my $max_time = shift @ARGV || undef;
#my $start_time = shift @ARGV || undef;
$max_time = undef if ($max_time && $max_time !~ /^\d+$/);
$start_time = undef if (defined($start_time) && $start_time !~ /^\d+$/);
$max_time += $start_time if (defined($start_time) && $max_time && $max_time < $start_time);
my %time_calls = ('yarnAts' => {'header' => ['Time', 'time_diff', 'yarnATSServerHdfsCalls'], 'time_data' => {}, 'outfile' => "$io_path/callerContext_yarnAts.csv"},
                  'tezAmAts' => { 'header' => [ 'Time', 'time_diff', 'TezAmATSHdfsCalls'], 'time_data' => {}, 'outfile' => "$io_path/callerContext_tezAmAts.csv"});
#my $gnu_plot_home='/usr/local/bin/gnuplot';
#my @header = ('Time', 'time_diff');
sub getATS_HDFScalls{
    my @files = glob("$input_pattrn"); #  glob("$input_path/hdfs-audit.log*");
    for my $file (@files){
        next unless $file;
        print $file,"\n";
        my $z = new IO::Uncompress::UnXz $file;
        if($z){
            my $prev_time = undef;
            my $num_calls = 0;
            while(!$z->eof()){
                my $line = $z->getline();
                next unless $line;
                chomp($line);
                #next if $line =~ /^s*$/;
                #if($line=~ /^.*src=\/ats\/done.*callerContext=$caller_context.*$/ || $line =~ /^.*src=\/tmp\/ats\/active.*callerContext=$caller_context.*$/){ #2015-11-1914:01:59,839)
                if ($line =~ /^.*src=\/ats\/done.*callerContext=yarn_ats_server_v1_5.*$/ || $line =~ /^.*src=\/tmp\/ats\/active.*callerContext=yarnATSServerHdfsCalls.*$/ || $line =~ /^.*src=\/ats\/done.*callerContext=tez_app:application.*$/ || $line =~ /^.*src=\/tmp\/ats\/active.*callerContext=tez_app:application.*$/){
                    my $tcaller = undef;
                    $tcaller = 'yarnAts' if($line =~ /^.*src=\/ats\/done.*callerContext=yarn_ats_server_v1_5.*$/ || $line =~ /^.*src=\/tmp\/ats\/active.*callerContext=yarnATSServerHdfsCalls.*$/);
                    $tcaller = 'tezAmAts'  if($line =~ /^.*src=\/ats\/done.*callerContext=tez_app:application.*$/ || $line =~ /^.*src=\/tmp\/ats\/active.*callerContext=tez_app:application.*$/);  
                    next unless($tcaller);
                    if ($line =~ /^\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),(\d{3}).*$/){
                        #print "$1 $2  $line\n";
                        my $tm_secs = int(str2time($1));
                        #my $tm_msecs = $tm_secs * 1000 + int($2);
                        #print "$1 $2 $tm_secs\n";
                        if (exists $time_calls{$tcaller}->{'time_data'}{$tm_secs}){# $time_calls{$tm_secs}){
                            $time_calls{$tcaller}->{'time_data'}{$tm_secs} = $time_calls{$tcaller}->{'time_data'}{$tm_secs} + 1; 
                            #$time_calls{$tm_secs} = $time_calls{$tm_secs} + 1;
                        }else{
                            $time_calls{$tcaller}->{'time_data'}{$tm_secs} = 1 ;
                            #$time_calls{$tm_secs} = 1;
                        }
                    }
                }
            }
            #close($in_fh)
            $z->close();
        }
    }
    return
    #my $cmd = "grep \"callerContext=yarn_ats_server_v1_5\" $input_path/hdfs-audit.log* | egrep \"/ats/done|/tmp/ats/active\" |cut -f2,3,4 -d:| awk '{print \$1\$2}'";

}

sub plot_data{
    my ($location,$type_caller,$outfile_prefix,$header2) = @_;
    print $location,"\n";
    (my $pfile = $location) =~ s/csv$/lines.gnuplot/;
    #unlink($pfile) if (-e $pfile);
    $pfile = dirname($pfile)."/".$outfile_prefix.".".basename($pfile)if($outfile_prefix);
    unlink($pfile) if (-e $pfile);
    if (open (my $out_fh,">$pfile")){
        (my $png_file = $pfile) =~ s/gnuplot$/svg/;
        unlink($png_file) if(-e $png_file);
        my $g_name = sprintf "%s",($type_caller =~ /tez/i?"TezAmATSHdfsCalls":"yarnATSServerHdfsCallls");
        print $out_fh "reset;\nset output \"$png_file\";\ndatafile = \"$location\";\ntitle_000 = \" #",$g_name,"\";\n";
        print $out_fh "set datafile separator \",\";\nset grid; ;\nset key autotitle columnheader;\nset term svg size 1280,1280;\nset xlabel \"Time(secs)\";\n\n";
        print $out_fh "\nset multiplot layout 1,1 rowsfirst title \"".basename($location)."\";\n";
        print $out_fh "set title \"".$header2."\";\nset style data linespoints;\nset ylabel \"".$header2."\";\nplot datafile using 2:3 t title_000;\n\n";
        print $out_fh "set nomultiplot;\nexit;\n"; close($out_fh);
        print "- WARNING Failed Command $gnu_plot_home $pfile\n" unless (system("$gnu_plot_home $pfile 2>&1") == 0);
    }else {
        print "- Failed to open file $pfile for writing due to  $!";
    }
}

sub parse_plotATSHDFSCalls{
    getATS_HDFScalls;
    print " ", join(" ",keys %time_calls),"\n";
    for my $caller (keys %time_calls){
        next unless($caller);
        my $outfile = $time_calls{$caller}->{'outfile'};
        my $tcs = $time_calls{$caller}->{'time_data'};
        my $header = $time_calls{$caller}->{'header'};
        print "$caller ", join(" ", keys %{$time_calls{$caller}})," ", join(" ", values %{$time_calls{$caller}}),"\n";
        #next;
        if(open(my $out_fh,">$outfile")){
            if (defined($period) && $period > $default_period){
                my $s_time = undef;
                my $prev_time = undef;
                my $num_calls = 0;
                #my $prev_diff = undef;
                #my $tcs = $time_calls{$caller}->{'time_data'};
                for my $k (sort keys %$tcs){
                    unless(defined($prev_time)){
                        $prev_time = $k;
                        $s_time = $k;
                        print $out_fh  join(',',@$header),"\n"; # "Time, time_diff,", ($type_caller =~ /tez/i?"TezAmATSHdfs":"yarnATSServerHdfs"),"Calls\n";
                        #print "$prev_time,0,0\n";
                        #$num_calls = $time_calls{$prev_time}; 
                    }
                    my $diff = $k - $prev_time;
                    #$prev_diff = $diff unless(defined($prev_diff));
                    #print "$diff, $period\n";
                    if ($diff < $period){
                        $num_calls += $tcs->{$k};
                    }else{
                        my $t_diff = $k - $s_time;
                        if ($start_time){
                            if($start_time < $t_diff){
                                #print sprintf "$k,%d,$num_calls\n", int($t_diff/60);
                                print $out_fh sprintf "$k,%d,$num_calls\n", int($t_diff/60);
                                last if($max_time && $t_diff > $max_time);
                            }
                        }else{
                            #print sprintf "$k,%d,$num_calls\n", int($t_diff/60);
                            print $out_fh sprintf "$k,%d,$num_calls\n", int($t_diff/60);
                            last if($max_time && $t_diff > $max_time);
                        }
                        $prev_time = $k;
                        $num_calls = $tcs->{$k};
                        #$prev_diff = $diff;
                    }
                }
            }else{
                my $s_time = undef;
                for my $k (sort keys %$tcs){
                    unless (defined($s_time)){
                        $s_time = $k;
                        print $out_fh  join(',',@$header),"\n";
                        #print $out_fh "Time, time_diff,", ($type_caller =~ /tez/i?"TezAmATSHdfs":"yarnATSServerHdfs"),"Calls\n";
                        #print $out_fh "Time, time_diff,Calls\n";
                        #print "$s_time,0,0\n";
                    }
                    my $t_diff = $k - $s_time;
                    #print "$start_time $t_diff ", ($start_time < $t_diff)?"yes":"no","\n";
                    if ($start_time){ 
                        if($start_time < $t_diff){
                            #print sprintf "$k,%d,$tcs->{$k}\n",int($t_diff);
                            print $out_fh sprintf "$k,%d,$tcs->{$k}\n",int($t_diff);
                            last if ($max_time && $t_diff> $max_time);
                        }
                    }else{
                        #print sprintf "$k,%d,$tcs->{$k}\n",int($t_diff);
                        print $out_fh sprintf "$k,%d,$tcs->{$k}\n",int($t_diff);
                        last if ($max_time && $t_diff> $max_time);
                    }
                }
            }
            close($out_fh);
            #%time_calls = ();
            $time_calls{$caller}->{'time_data'} = {};
            plot_data($outfile,$caller,undef, $header->[2]);
            $time_calls{$caller}->{'header'} = [];
            $time_calls{$caller}->{'outfile'} = undef;
        }else{
            print STDERR "ERROR: Failed to open file $caller $outfile due to: $!\n";
        }
        #%time_calls = ();
        $time_calls{$caller} = {};
    }
}
parse_plotATSHDFSCalls();
exit 0;
