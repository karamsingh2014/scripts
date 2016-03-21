use strict;
use warnings;
#use IO::Uncompress::Bunzip2 qw/$Bunzip2Error/ ;
#use IO::Uncompress::UnXz qw/$UnXzError/ ;
use IO::File;
use Data::Dumper;
my $ip = shift @ARGV || undef;
my $op = shift @ARGV || undef;
die "Input file not provided\n$0 <input_file> [output_file]\n" unless($ip and -f $ip); 
my $xzOut = undef;
if($op){
   $xzOut = IO::File->new("> $op");
   print STDERR "Failed to open file $op: $!\n" unless(defined($xzOut));
   $xzOut->autoflush(1) if(defined($xzOut));
}

my %time_user_queue = ();
#my $z = new IO::Uncompress::UnXz $ip 
#    or die "IO::Uncompress::UnXz failed: $UnXzError\n";
open(FH, $ip) or die "Failed to open file $ip due to: $!\n";
while(my $line = <FH>){
    next unless ($line);
    chomp($line);
    next if ($line =~ /^\s*$/ || $line =~ /^\s*======+\s*$/ || $line =~ /^\s*nohup:\s*ignoring\s*input\s*$/i);
    #print $line,"\n";
    my @vals = split(/\s/,$line);
    if ( $vals[7] =~ /RUNNING/i){
        my ($ncontaienrs, $napps, $nvcores, $mem, $n_qusage, $n_ucsage, $n_r_rqts) = (0,0,0,0, 0.0, 0.0);
        my $ts = $vals[0] + 0;
        #if($ts == 28) {
        #    print "$ts $vals[0] $vals[5] $vals[6] $vals[4] $vals[9] $vals[10] $vals[12] $vals[11] $vals[15] $vals[19]\n";
        #}
        $time_user_queue{$ts} = {} unless (exists $time_user_queue{$ts});
        $time_user_queue{$ts}->{$vals[5]} = {} unless (exists $time_user_queue{$ts}->{$vals[5]});
        unless (exists $time_user_queue{$ts}->{$vals[5]}{$vals[6]}){
            $time_user_queue{$ts}->{$vals[5]}{$vals[6]} = {'num_apps_running' => $napps, 'num_containers_running' => $ncontaienrs, 'total_vcores' => $nvcores, 'total_memory' => $mem,
                                                           'total_resource_requests' => $n_r_rqts, 'queue_usage' => $n_qusage, 'cluster_usage' => $n_ucsage};
        }
        if (exists $time_user_queue{$ts}->{$vals[5]}{$vals[6]}){
            $napps = $time_user_queue{$ts}->{$vals[5]}{$vals[6]}{'num_apps_running'};
            $ncontaienrs = $time_user_queue{$ts}->{$vals[5]}{$vals[6]}{'num_containers_running'};
            $nvcores = $time_user_queue{$ts}->{$vals[5]}{$vals[6]}{'total_vcores'};
            $mem = $time_user_queue{$ts}->{$vals[5]}{$vals[6]}{'total_memory'};
            $n_qusage = $time_user_queue{$ts}->{$vals[5]}{$vals[6]}{'queue_usage'};
            $n_ucsage = $time_user_queue{$ts}->{$vals[5]}{$vals[6]}{'cluster_usage'};
            $n_r_rqts = $time_user_queue{$ts}->{$vals[5]}{$vals[6]}{'total_resource_requests'};
            unless (exists $time_user_queue{$ts}->{$vals[5]}{$vals[6]}{$vals[4]}){
                $napps += 1; 
                $ncontaienrs += $vals[8] if ($vals[8] =~ /^\d+$/); #$vals[9] if ($vals[9] =~ /^\d+$/);
                $nvcores += $vals[11] if ($vals[11] =~ /^\d+$/); #$vals[12] if ($vals[12] =~ /^\d+$/);
                $mem += $vals[10] if ($vals[10] =~ /^\d+$/); #$vals[11] if ($vals[11] =~ /^\d+$/); 
                $n_qusage += $vals[9] if ($vals[9] =~ /^\d+$/); #$vals[10] if ($vals[10] =~ /^\d+$/); 
                $n_ucsage += $vals[14] if ($vals[14] =~ /^\d+$/); #$vals[15] if ($vals[15] =~ /^\d+$/); 
                $n_r_rqts += $vals[18] if ($vals[18] =~ /^\d+$/); #$vals[19] if ($vals[19] =~ /^\d+$/);
            }
            #if ($ts == 28){
            #    print "$ts $vals[5] $vals[6] $vals[4] $vals[9] $vals[10] $vals[12] $vals[11] $vals[15] $vals[19] $napps $ncontaienrs\n"; 
            #}
            $time_user_queue{$ts}->{$vals[5]}{$vals[6]} = { 'num_apps_running' => $napps, 'num_containers_running' => $ncontaienrs, 'total_vcores' => $nvcores, 'total_memory' => $mem,
                                                            'total_resource_requests' => $n_r_rqts, 'queue_usage' => $n_qusage, 'cluster_usage' => $n_ucsage,
                                                            $vals[4] => { 'running_containers' => $vals[9], 'q_usage' => $vals[10], 'vcores' => $vals[11], 'memory' => $vals[12],
                                                            'c_usage' => $vals[15], 'resource_requests' => $vals[19]}};
            #$time_user_queue{$ts}->{$vals[5]}{$vals[6]}{'num_apps_running'} = $napps;
            #$time_user_queue{$ts}->{$vals[5]}{$vals[6]}{'num_containers_running'} = $ncontaienrs;
            #$time_user_queue{$ts}->{$vals[5]}{$vals[6]}{'total_vcores'} = $nvcores;
            #$time_user_queue{$ts}->{$vals[5]}{$vals[6]}{'total_memory'} = $mem;
            #$time_user_queue{$ts}->{$vals[5]}{$vals[6]}{'total_resource_requests'} = $n_r_rqts;
            #$time_user_queue{$ts}->{$vals[5]}{$vals[6]}{'queue_usage'} = $n_qusage;
            #$time_user_queue{$ts}->{$vals[5]}{$vals[6]}{'cluster_usage'} = $n_ucsage; 
            #$time_user_queue{$ts}->{$vals[5]}{$vals[6]}{$vals[4]}{'running_containers'} = $vals[9];
            #$time_user_queue{$ts}->{$vals[5]}{$vals[6]}{$vals[4]}{'q_usage'} = $vals[10];
            #$time_user_queue{$ts}->{$vals[5]}{$vals[6]}{$vals[4]}{'vcores'} = $vals[11];
            #$time_user_queue{$ts}->{$vals[5]}{$vals[6]}{$vals[4]}{'memory'} = $vals[12];
            #$time_user_queue{$ts}->{$vals[5]}{$vals[6]}{$vals[4]}{'c_usage'} = $vals[15]; 
            #$time_user_queue{$ts}->{$vals[5]}{$vals[6]}{$vals[4]}{'resource_requests'} = $vals[19];
        }
        #$time_user_queue{$vals[0]} = {$vals[5] => {$vals[6] => {$vals[4] => { 'running_containers' => $vals[9], 'q_usage' => $vals[10], 
        #                                                                      'vcores' => $vals[11], 'memory' => $vals[12], 'c_usage' => $vals[15], 'resource_requests' => $vals[19]}}, 
        #                                                      'num_apps_running' => $napps, 'num_containers_running' => $ncontaienrs, 
        #                                                      'total_vcores' => $nvcores, 'total_memory' => $mem, 'total_resource_requests' => $n_r_rqts,
        #                                                      'queue_usage' => $n_qusage, 'cluster_usage' => $n_ucsage }};   
    }
}
close(FH);
#print Dumper %time_user_queue,"\n";
my $lSep = '=' x 115;
foreach my $ts(sort {$a <=> $b} keys %time_user_queue){
    if(defined($xzOut)){
        $xzOut->print("$lSep\n");
        printf "%10s|%14s|%10s|%6s|%10s|%6s|%10s|%10s|%14s|%14s|\n", "Time_diff","User","Queue","rApps","rContnrs","tVCore", "tMemory", "tResRqsts", "qUsg", "cUsg";
        $xzOut->printf("%10s|%14s|%10s|%6s|%10s|%6s|%10s|%10s|%14s|%14s|\n", "Time_diff","User","Queue","rApps","rContnrs","tVCore", "tMemory", "tResRqsts", "qUsg", "cUsg");
        $xzOut->print("$lSep\n");
    }else{
        print STDERR $lSep,"\n";
        print STDERR sprintf "%10s|%14s|%10s|%6s|%10s|%6s|%10s|%10s|%14s|%14s|\n", "Time_diff","User","Queue","rApps","rContnrs","tVCore", "tMemory", "tResRqsts", "qUsg", "cUsg";
        print STDERR $lSep,"\n";
    }
    #print STDERR "", join(" ",sort keys $time_user_queue{$ts}),"\n";
    #next;
    foreach my $user (sort keys $time_user_queue{$ts}){
        foreach my $queue(sort keys $time_user_queue{$ts}{$user}){
            my $t = $time_user_queue{$ts}{$user}{$queue};
            my $str = sprintf "%10d|%14s|%10s|%6d|%10d|%6d|%10d|%10d|%14d|%14d|\n", $ts, $user, $queue, $t->{'num_apps_running'}, $t->{'num_containers_running'}, $t->{'total_vcores'}, 
                                                             $t->{'total_memory'}, $t->{'total_resource_requests'}, $t->{'queue_usage'}, $t->{'cluster_usage'};
            if(defined($xzOut)){
                $xzOut->print($str);
            }else{
                print STDERR $str;
            }
            #my $str = sprintf "$ts $user $user %s %s %s %s ", $t->{'num_apps_running'}, $t->{'num_containers_running'}, $t->{'total_vcores'}, $t->{'total_memory'}; 
            #$str .= sprintf "%s %s %s\n", $t->{'total_resource_requests'}, $t->{'queue_usage'}, $t->{'cluster_usage'};
            #print STDERR $str;
        }
    }
}
$xzOut->close() if(defined($xzOut));
