use strict;
use warnings;
#use IO::Uncompress::Bunzip2 qw/$Bunzip2Error/ ;
#use IO::Uncompress::UnXz qw/$UnXzError/ ;
#use IO::Compress::Xz qw/$XzError/;
use IO::File;

use Data::Dumper;
my $ip = shift @ARGV || undef;
my $op = shift @ARGV || undef;
die "Input file not provided\n$0 <input_file> [output_file]" unless($ip and -f $ip);
my $xzOut = undef;
if ($op){
     $xzOut = IO::File->new(">$op);
     print STDERR "Open $op failed: $!\n" unless (defined($xzOut));
     $xzOut->autoflush() if (defined($xzOut));
}

my %time_user_queue = ();
my $z= IO::File->new("< $ip); 
die "Open $ip failed: $!\n"unless(defined($z));
my @header = qw/diff time(secs) date time id user queue state rContrs qPrc alcMB alVC applType progress clUsgPrc nmAMCntPrem premRsMB premRsVC rsRqsts/;
my $tmrwSep = '=' x 190;
sub print_formatted($;$){
    my $arr = shift @_;
    my $printSep = shift @_;
    return unless $arr;
    my $str = '|';
    foreach(my $i = 0; $i < scalar(@$arr); $i++){
        if($i == 0){ $str = sprintf "$str%-6s|", $arr->[$i]; next;}
        if($i == 1){ $str = sprintf "$str%-13s|", $arr->[$i]; next;}
        next if($i == 2 || $i == 3 || $i == 11 || $i == 17);
        if($i == 4){ $str = sprintf "$str%-30s|", $arr->[$i]; next;}
        if($i == 5){ $str = sprintf "$str%-14s|", $arr->[$i]; next;}
        if($i == 6){ $str = sprintf "$str%-7s|", $arr->[$i]; next;}
        if($i == 7){ $str = sprintf "$str%-8s|", $arr->[$i]; next;}
        if($i == 8){ $str = sprintf "$str%-8s|", $arr->[$i]; next;}
        if($i == 9){ $str = sprintf "$str%-14s|", $arr->[$i]; next;}
        if($i == 10){ $str = sprintf "$str%-8s|", $arr->[$i]; next;}
        if($i == 12){ $str = sprintf "$str%-8s|", $arr->[$i]; next;}
        if($i == 13){ $str = sprintf "$str%-11s|", $arr->[$i]; next;}
        if($i == 14){ $str = sprintf "$str%-14s|", $arr->[$i]; next;}
        if($i == 15){ $str = sprintf "$str%-10s|", $arr->[$i]; next;}
        if($i == 16){ $str = sprintf "$str%-10s|", $arr->[$i]; next;}
        if($i == 18){ $str = sprintf "$str%-10s|", $arr->[$i]; next;}


    }
    if (defined($xzOut)){
        $xzOut->print("$tmrwSep\n") if ($printSep);
        $xzOut->print("$str\n");
        $xzOut->print("$tmrwSep\n") if ($printSep);
    }else{
        print STDERR $tmrwSep, "\n" if ($printSep);
        print STDERR $str,"\n";
        print STDERR $tmrwSep, "\n" if ($printSep);
    }
}
print_formatted(\@header, 1);
while(!$z->eof()){
    my $line = $z->getline();
    next unless ($line);
    chomp($line);
    next if ($line =~ /^\s*$/ || $line =~ /^\s*nohup:\s*ignoring\s*input\s*$/i);
    if ($line =~ /^\s*======+\s*$/){
        print_formatted(\@header, 1);
        next;
    }
    #print $line,"\n";
    my @vals = split(/\s/,$line);
    print_formatted(\@vals);
}
$z->close();
$xzOut->close() if(defined($xzOut));
#print Dumper %time_user_queue,"\n";

