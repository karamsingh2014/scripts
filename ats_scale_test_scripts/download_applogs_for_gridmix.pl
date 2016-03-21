use strict;
use warnings;
use Getopt::Long;
use Cwd 'abs_path';
#1443665985063
my @apps = ();
my $pattern ="./gm*.out";
#my @files = glob($pattern);
my $target_dir = "/grid/4/home/ksingh/folded_mg_trace/download_applogs";
sub help_msg{
    my $exit_status = shift @_ || 0;
    print "Usage $0: $0 <Options>\n\t -p | --glob-pattern=<GridMix output log files path and pattern e.g. ./gm*.out>\n\t -d | --download-dir=<path to download application logs>\n";
    exit $exit_status;
}
GetOptions ("glob-pattern|p=s" => \$pattern, "download-dir|d=s" => \$target_dir, "help|h" => sub {help_msg(0)}) or die("Error in command line arguments\n");
unless($pattern){
    print STDERR "ERROR: Glob pattern not specified\n";
    help_msg(1);
}

unless ($target_dir && -d $target_dir && -r $target_dir && -x $target_dir){
    print STDERR "Invalid Application log download directory specified to -d | --download-dir\n";
    help_msg(1);  
}
my @files = glob($pattern);
$target_dir = abs_path($target_dir);
sub app_list($){
    my $file_name=shift @_;
    if(open(FH, $file_name)){
        while(my $line=<FH>){
           #print $line;
           if($line =~ /^.*INFO gridmix\.JobMonitor: GRIDMIX\d+\s*\((job_\d+_\d+)\) success.*$/i || $line =~ /^.*INFO gridmix\.JobMonitor: GRIDMIX\d+\s*\((job_\d+_\d+)\) fail.*$/i){   
               if ($1){
                  my $appid = $1;
                  $appid =~ s/job/application/;
                  push @apps, $appid;

               }
           }
      }
      close(FH)
    } 
}

sub download_applogs(){
    foreach my $file (@files){
       app_list($file);
   }
   #sleep(20);
   foreach my $id (@apps){
     my $out_file= "$target_dir/${id}.log";
     if (-e $out_file && -s $out_file){
        print $id, "\n"; #next;
     }else{
        my $cmd = "yarn logs -applicationId $id >$out_file 2>/dev/null";
        if ($target_dir eq "/grid/4/home/ksingh/folded_mg_trace/download_applogs"){
            #$cmd = "source /grid/4/home/ksingh/setEnv.sh; yarn logs -applicationId $id >$out_file 2>/dev/null";
           $cmd = "source /grid/4/home/ksingh/setEnv.sh; $cmd";
        }
        system($cmd);
     }
   }
}

while(1){
    #sleep(300);
    download_applogs();
    @apps = ();
}
