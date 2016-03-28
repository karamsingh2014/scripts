#!/usr/bin/env perl
use strict;
use warnings;
use File::Basename;
use Getopt::Long;
use Net::Domain;
use Cwd 'abs_path';

my $app_logs_dir = undef;
my $pfile = undef; #"/grid/4/home/ksingh/folded_mg_trace/Tez_t.py";
my $out_file =  'ats_ws_m.out';
my $ats_web_addr = undef; 
my $my_hostname = Net::Domain::hostfqdn; 
my $luser = getpwuid($<);
sub help_msg{
    my $exit_status = shift @_ || 0;
    print "Usage $0: $0 <Options>\n\t -d | --app_logs_dir=<Path of application logs dir>\n\t -f | --ats-ws-python-file=<path of python file which call ATS WS>\n\t";
    print " -o | --output-file=<output-file-path>\n\t -a | --ats-addr=<ATS/Timeline Server Web address>\n\t -u |--user=<User who will bw treated as app owner/yarn admin>\n";
    exit $exit_status;
}
GetOptions ("app-logs-dir|d=s" => \$app_logs_dir, "ats-ws-python-file|f=s"   => \$pfile, "output-file|o=s"  => \$out_file, 
            "ats-addr|a=s" => \$ats_web_addr, "user|u=s" =>\$luser,
            "help|h" => sub {help_msg(0)}) or die("Error in command line arguments\n");

#print $app_logs_dir,"\n";
#print $pfile,"\n";
#print $out_file,"\n";
#print basename($out_file),"\n";
#print dirname($out_file),"\n";
#print $luser,"\n";
#exit 0;
unless ($app_logs_dir && -d $app_logs_dir && -r $app_logs_dir && -x $app_logs_dir) {
 print STDERR "ERROR: Invalied application logs dir. Application logs dir Not provided/Not a directory/Not readable/Not Executable\n";
 help_msg(1);
}
$app_logs_dir = abs_path($app_logs_dir);

unless ($pfile && -e $pfile && -r $pfile){
    print STDERR "ERROR: Invalid ATS WS python file. ATS WS pytho file Not provided/Not Exist/Not readable\n";
    help_msg(1);
}

unless($ats_web_addr){
    print STDERR "ERROR: Invalid ATS web address\n";
    help_msg(1);
}
$pfile = abs_path($pfile);
unless ($out_file){
    print STDERR "ERROR: Invalid ouput file\n";
    help_msg(1);
}
my $out_dir = abs_path(dirname($out_file));
unless (-d $out_dir && -w $out_dir){
    print STDERR "ERROR: Directory $out_dir where ouput file to written is Either not a directory or Not writable\n";
    help_msg(1);
}
#exit 0;
my @files = glob("$app_logs_dir/application*.log");
my @apps = ();
sub app_list($){
    my $file_name=shift @_;
    if(open(FH, $file_name)){
        while(my $line=<FH>){
           if($line =~ /^.*INFO gridmix\.JobMonitor: GRIDMIX\d+\s*\((job_\d+_\d+)\) success.*$/i || $line =~ /^.*INFO gridmix\.JobMonitor: GRIDMIX\d+\s*\((job_\d+_\d+)\) fail.*$/i){   
               if ($1){
                  my $appid = $1;
                  $appid =~ s/job/application/;
                  push @apps, $appid; # if (!exists $already_analysed{$appid});

               }
           }
      }
      close(FH)
    } 
}

sub process_apps(){
    foreach my $file (@files){
       my $appid = basename($file,".log");
       push @apps, $appid;
   }
   foreach my $id (@apps){
        system("/usr/bin/python2.7 $pfile -l $app_logs_dir -a $ats_web_addr -u $luser $id >>$out_file 2>&1");
   }
}

while(1){
    process_apps();
    @apps = ();
    sleep(50);
    #last;
}
