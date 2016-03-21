use strict;
use warnings;
use File::Basename;
use Getopt::Long;
use List::Util qw/min max/;
use constant TRUE  => 1;
use constant FALSE => 0 ;
sub help_msg{
    my $exit_status = shift @_ || 0;
    print "Usage $0: $0 <Options>\n\t -p | --input-glob-pattern=<[REQUIRED] Path of NameNode metrics dir with glob pattern e.g /tmp/namenode-metrics*.out>\n\t";
    print " -m | --max-time=<Max time in seconds to be considered to stop parsing>\n\t";
    print " -s | --start-time=<Start time to start parsing for plotting>\n";
    exit $exit_status;
}

my $input_location_pattrn = undef;
my $max_time =  undef;
my $start_time = undef;

GetOptions ("input-glob-pattern|i=s" => \$input_location_pattrn, "max-time|m=i" => \$max_time, "start-time|s=i"  => \$start_time,
            "help|h" => sub {help_msg(0)}) or die("Error in command line arguments\n");
unless ($input_location_pattrn && -d dirname($input_location_pattrn) && -r dirname($input_location_pattrn) && -x dirname($input_location_pattrn)) {
     print STDERR "ERROR: Invalied NameNode metrics dir path of patter. Not provided/Not a directory/Not readable/Not Executable\n";
     help_msg(1);
  }
$max_time = undef if ($max_time && $max_time !~ /^\d+$/);
$start_time = undef if (defined($start_time) && $start_time !~ /^\d+$/);
$max_time += $start_time if (defined($start_time) && $max_time && $max_time < $start_time);
my $period=10;
#my @header= ();#qw/time_diff time time_formatted RpcQueueTimeNumOps RpcQueueTimeAvgTime RpcProcessingTimeNumOps RpcProcessingTimeAvgTime NumOpenConnections/;
my $gnu_plot_home='/usr/local/bin/gnuplot';
my $max_time_value_found = 0;
sub plot_csv_file{
        my ($location,$header,$outfile_prefix) = @_;
        (my $pfile = $location) =~ s/csv$/lines.gnuplot/;
        #unlink($pfile) if (-e $pfile);
        my $num_rows = 2;
        $pfile = dirname($pfile)."/".$outfile_prefix.".".basename($pfile)if($outfile_prefix);
        unlink($pfile) if (-e $pfile);
        if (open (my $out_fh,">$pfile")){
                (my $png_file = $pfile) =~ s/gnuplot$/svg/;
                unlink($png_file) if(-e $png_file);
                my $mapred_file  = 0;
                $mapred_file = 1 if ($location =~ /heartbeats/ || $location =~ /containers/);
                $num_rows = 1 if ($pfile =~ /heartbeats/ || ($pfile !~ /rpc/i && $pfile !~ /aggregate/i && $pfile !~ /namenode/i));
                print $out_fh "reset;\nset output \"$png_file\";\ndatafile = \"$location\";\ntitle_000 = \" #NameNodeRPC\";\n";
                print $out_fh "set datafile separator \",\";\nset grid; ;\nset key autotitle columnheader;\nset term svg size 1280,",($num_rows == 1)?"600":"1280",";\nset xlabel \"Time(secs)\";\n\n";
                print $out_fh "\nset multiplot layout ",($num_rows == 1)?"1,1":"2,1"," rowsfirst title \"".basename($location)."\";\n";
                for(my $i = 1; $i < scalar(@$header);$i++){
                        my $flag = FALSE;
                        if ($header->[$i] =~ /^heartbeats/ || $header->[$i] =~ /^RpcQueueTimeNumOps/ || $header->[$i] =~ /RpcProcessingTimeNumOps/ || $header->[$i] =~ /^AllocatedContainers/ || $header->[$i] =~ /AggregateContainersAllocated/ || $header->[$i] =~ /AggregateContainersReleased/){
                                if ($header->[$i] =~ /^AllocatedContainers/ && $pfile !~ /aggregate/i){
                                        $flag = TRUE;
                                }elsif ($header->[$i] =~/\(secs\)/){
                                        $flag = TRUE;
                                }
                        }
                        next unless ($flag);
                        next if ($pfile !~ /aggregate/i && $header->[$i] =~ /Aggregate/);
                        print $out_fh "set title \"",($mapred_file)?"ResourceManager":"RPC"," ".$header->[$i]."\";\nset style data linespoints;\nset ylabel \"".$header->[$i]."\";\nplot datafile using 1:",$i+1," t title_000;\n\n";
                }
                print $out_fh "set nomultiplot;\nexit;\n"; close($out_fh);
                print " WARNING Failed Command $gnu_plot_home $pfile\n" unless (system("$gnu_plot_home $pfile 2>&1") == 0);
                #unlink $pfile;
        }else {
                warn "- Failed to open file $pfile for writing due to  $!";
        }
}

sub write_data_into_csv {
        my ($data_with_last_3_periods,$period,$header,$out_fh) = @_;
        return if $max_time_value_found;
        foreach my $time_stamp (sort {$a <=> $b} keys %$data_with_last_3_periods){
                my $lines_with_same_timestamp  = $data_with_last_3_periods->{$time_stamp};
                my $prev_value = {};
                foreach my $data (@$lines_with_same_timestamp){
                    last if $max_time_value_found;
                    foreach my $k (keys %$data){
                        next unless(defined($data->{$k}));
                        #next if(defined($start_time) && $k eq 'Time' && $data->{$k} < int($start_time));
                        if (defined($prev_value->{$k})){
                            next if ($k eq 'Time');
                            next if ($k =~ /\(secs\)/);
                            $prev_value->{$k} = $data->{$k};
                            $prev_value->{"$k/$period(secs)"} += $data->{"$k/$period(secs)"};
                        }else{
                            $prev_value->{$k} = $data->{$k};
                        }
                    }
                }
                next if(defined($start_time) && defined($prev_value->{'Time'}) && $prev_value->{'Time'} < int($start_time));
                $max_time_value_found = 1 if ($max_time && defined($prev_value->{'Time'}) && $prev_value->{'Time'}>= int($max_time));
                my @vals = map { defined($prev_value->{$_})?$prev_value->{$_}:'' } @$header;
                print $out_fh join(",",@vals),"\n";
                last if($max_time_value_found);
         }
}
sub write_csv_from_metrics{
        my ($location) = @_;
        print $location,"\n";
        #my @mfiles = glob("$location/hadoop-metrics-rpc*.log");
        my @mfiles = glob($location); #glob("$location/namenode-metrics*.out");  
        print join("\n",@mfiles),"\n";
        my @file_types = ("RPC");
        my $util_types = {
                RPC =>{file_name => 'namenode-metrics.*\.out$',colums => {'RpcQueueTimeNumOps' => 1, 'RpcProcessingTimeNumOps' => 1, "RpcQueueTimeNumOps/$period(secs)" => 1, "RpcProcessingTimeNumOps/$period(secs)" => 1 }},
                #RPC =>{file_name => 'rpc.*\.log$',colums => {'RpcQueueTimeNumOps' => 1, 'RpcProcessingTimeNumOps' => 1, "RpcQueueTimeNumOps/$period(secs)" => 1, "RpcProcessingTimeNumOps/$period(secs)" => 1 }},
                MAPRED => {file_name => 'yarn.*\.log$', colums => { 'heartbeats' => 1 , "heartbeats/$period(secs)" => 1 }},
                YARN => {file_name => 'yarn.*\.log$', colums => {
                                                'AllocatedContainers' => 1, 'AggregateContainersAllocated' => 1,'AggregateContainersReleased' => 1,
                                                "AggregateContainersAllocated/$period(secs)" => 1,"AggregateContainersReleased/$period(secs)" => 1
                                                }
                        },
                GET_REFERENCE_TIME =>sub {
                        my ($cline,$h,$d) = @_; my $k = 'Time';
                        if ($cline =~/^\s*(\d+)\s*/){
                                my $ms_time = $1;
                                #print "$ms_time, ", ($h->{$k}?$h->{$k}:'no'),"\n";
                                unless (defined( $h->{$k})){ $h->{$k} = $ms_time; }
                                #print "$ms_time, ", ($h->{$k}?$h->{$k}:'no'),"\n";
                                $d->{$k} = sprintf "%.0f",($ms_time - $h->{$k})/1000;
                        }else{
                                unless (defined( $h->{$k})){ $h->{$k} = $period;}
                                else { $h->{$k} += $period;}
                                $d->{$k} = $h->{$k};
                        }
                },
                GET_VALUE_BY_PREDIOD_DIFF =>sub {
                        my ($k,$h,$p,$d) = @_; my $key = "$k/$period(secs)";
                        unless(defined($p->{$key})){
                                $d->{$key} = $d->{$k};
                        } else{
                                $d->{$key} = $d->{$k} - $p->{$key};
                        }
                        $p->{$key} = $d->{$k} if($d->{$k});
                },
                OUTFILE =>sub {
                        my ($file,$t) = @_;
                        unlink("$file.csv") if (-e "$file.csv");
                        return "$file.csv";
                        $file =~ s/log$/csv/;
                        $file =~ s/hadoop-metrics-//;
                        $file =~ s/yarn/heartbeats/ if ($t =~ /MAPRED/i);
                        $file =~ s/yarn/containers/ if($t =~ /YARN/);
                        return $file;
                }
        };
        foreach my $mfile(@mfiles){
                next unless ( $mfile && -e $mfile);
                my $file_type = undef;
                foreach my $ft (@file_types){
                        print "$mfile ",$util_types->{$ft}, "\n";
                        if ($mfile =~ /$util_types->{$ft}->{file_name}/){
                                $file_type = $ft;
                                last;
                        }
                }
                next unless ($file_type);
                my ($in_fh,$out_fh) = (undef,undef);
                unless(open ($in_fh,"< $mfile")){
                        warn "- Failed to open file $mfile for reading due to $!";
                        next;
                }
                my $csv_file = $util_types->{OUTFILE}($mfile,$file_type);
                unless (open ($out_fh,">$csv_file")){
                        warn "- Failed to open file $csv_file for writing due to $! ";
                        next;
                }
                my ($prev_value,$header) = ({},['Time']);
                my $prev_time_value = -1;
                my $data_with_last_3_periods = {};
                push @$header,sort keys %{$util_types->{$file_type}->{'colums'}};
                print $out_fh join(",",@$header),"\n";
                my $data_found = FALSE;
                while(my $line=<$in_fh>){
                        chomp($line); my $data = {};
                        next if ($line =~ /detailed-metrics:/);
                        next if ($csv_file =~ /containers/ && $line !~/Queue=root,\s*Context=yarn,/i);
                        $util_types->{GET_REFERENCE_TIME}($line,$prev_value,$data);
                        while($line=~/(\S+)=([^, ]+)/g){
                                if ($util_types->{$file_type}->{colums}->{$1}){
                                        $data->{$1} = $2;
                                        $util_types->{GET_VALUE_BY_PREDIOD_DIFF}($1,$header,$prev_value,$data);
                                        #print  " ", ($data->{$1}?$data->{$1}:"no")," $line\n";
                                        $data_found = TRUE if ($data->{$1});
                                }
                        }
                        if ($data->{'Time'} > $prev_time_value){
                                if (scalar(keys %$data_with_last_3_periods) < 3){
                                        push @{$data_with_last_3_periods->{$data->{'Time'}}},$data;
                                }else{
                                        write_data_into_csv($data_with_last_3_periods,$period,$header,$out_fh);
                                        $data_with_last_3_periods = {};
                                        push @{$data_with_last_3_periods->{$data->{'Time'}}},$data;
                                }
                                 $prev_time_value = $data->{'Time'};
                        }else {
                                push @{$data_with_last_3_periods->{$data->{'Time'}}},$data;
                        }
                }
                write_data_into_csv($data_with_last_3_periods,$period,$header,$out_fh);
                close ($in_fh); close ($out_fh);
                if ($data_found == TRUE){
                    plot_csv_file($csv_file,$header);
                }
        }
}

#write_csv_from_metrics(dirname($input_location));
write_csv_from_metrics($input_location_pattrn); #$input_location);
exit 0;

