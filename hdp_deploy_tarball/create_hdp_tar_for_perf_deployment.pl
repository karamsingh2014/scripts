use strict;
use warnings;
use File::Basename;
use Cwd;
#use POSIX ":sys_wait_h";
use Getopt::Long;
use Net::Domain;

use constant TRUE => 1;
use constant FALSE => 0;

my $real_uid = $<;
my $effective_uid = $>;

die "$0: Can only be invoked by user root\n" unless ($real_uid == 0 and $effective_uid == 0);

my $base_url = "http://s3.amazonaws.com/dev.hortonworks.com/HDP/centos6/2.x/BUILDS";
my $hdp_version =  undef; #shift @ARGV; #'2.5.0.0-37'
my $download_dir  = undef; #shift @ARGV;
my $tarball_dir = undef; #hift @ARGV;
my $hostname = Net::Domain::hostfqdn;
my $cur_dir = getcwd;
my $hdp_version_subs = undef; #$hdp_version;
#$hdp_version_subs =~ s/./_/g;
my $repo_file = "hdpbn.repo";

my $hdp_dir_base = undef ; #"$download_dir/usr/hdp";
my $hdp_dir = undef ; #"$download_dir/usr/hdp/$hdp_version";
sub determine_success($;$){
    my $exit_code = shift @_;
    my $change_to_pwd = shift @_ || TRUE;
    chdir($cur_dir) if($change_to_pwd);
    $exit_code = $exit_code >> 8;
    my $signalled = $exit_code & 8;
    return TRUE if ($exit_code == 0 && $signalled == 0);
    return FALSE;
}

sub download_repo(){
    my $yum_repo_dir = "/etc/yum.repos.d";
    if (chdir($yum_repo_dir)){
        system("rm -f $repo_file") if (-e "$repo_file");
        my $cmd = "wget ${base_url}/${hdp_version}/${repo_file}";
        my $exit_code = system($cmd);
        return determine_success($exit_code, TRUE);
    }
    return FALSE
        
}

sub download_tarballs(){
    if (chdir($download_dir)){
    	my $cmd = "yum install --downloadonly --downloaddir=$download_dir hadoop* tez* hive-jdbc.noarch hive-metastore.noarch hive-server2.noarch oozie* spark* extjs "; 
    	$cmd .= "hive_${hdp_version_subs}.noarch hive_${hdp_version_subs}-jdbc.noarch hive_${hdp_version_subs}-metastore.noarch hive_${hdp_version_subs}server.noarch";
    	my $exit_code = system($cmd);
    	$cmd = "rm -rf p* q* r* spax* system* u* z* m* l* a* b* c* g* hic* har* f* hadoop*source* hadoop*httpfs* hadoop*doc* hdp-select* hadoop*libhdfs* hadoop*fuse* hadoop*conf* hadoop*client*  hive*server* hive*meta*  hadoop*hdfs*zkfc* hadoop*hdfs*node* hadoop*mapreduce*historyserver* hive-jdbc*  hadoop*yarn*manager* hadoop*yarn*server* hadoop*yarn*proxy* hive-* tez-* hadoop-*";
    	system($cmd);
    	if (determine_success($exit_code, FALSE)){
    	   $cmd = "cd $download_dir; pwd; for f in *;do  rpm2cpio \$f| cpio -idmv;done";
           print $cmd,"\n";
    	   my $exit_code1 = system($cmd);
           print "$exit_code1, \n";
    	   return determine_success($exit_code1, TRUE);
    	}
    }
    return FALSE;
}

sub setup_hadoop_for_tarball(){
    my $errors = 0;
	if (chdir($hdp_dir)){
		my ($hadoop_home, $hadoop_hdfs_home) = ("$hdp_dir/hadoop", "$hdp_dir/hadoop-hdfs"); 
		my ($hadoop_mr_home, $hadoop_yarn_home, $tez_home) = ("$hdp_dir/hadoop-mapreduce", "$hdp_dir/hadoop-yarn", "$hdp_dir/tez");
		system("rm -rf $hdp_dir/etc");
		my $cmd = "unlink $hadoop_home/conf; rm -rf $hadoop_home/etc; for f in hadoop hdfs yarn mapred; do mv $hadoop_home/bin/\$f $hadoop_home/bin/\${f}.ori; done;";
		$cmd .= " /bin/cp $hadoop_home/bin/hadoop.distro $hadoop_home/bin/hadoop";
                print $cmd,"\n";
		my $exit_code = system($cmd);
		system("ls -lh $hadoop_home/bin/; ls -lh $hadoop_home");
		if (determine_success($exit_code, FALSE)){
			my $hadoop_layout = "$hadoop_home/libexec/hadoop-layout.sh";
			my $hadoop_config = "$hadoop_home/libexec/hadoop-config.sh";
			if (open(LFHIN,"<$hadoop_layout")){
				my @lines = <LFHIN>;
				close(LFHIN);
				foreach my $line (@lines){
					if( defined($line)){
						$line = "HADOOP_LIBEXEC_DIR=\${HADOOP_LIBEXEC_DIR:-\"/grid/4/hadoop/libexec\"} # $line" if ($line =~ /^\s*HADOOP_LIBEXEC_DIR=/);
						$line = "HADOOP_CONF_DIR=\${HADOOP_CONF_DIR:-\"/grid/0/hadoopConf\"} # $line" if ($line =~ /^\s*HADOOP_CONF_DIR=/);
						$line = "HADOOP_COMMON_HOME=\"/grid/4/hadoop\" # $line" if ($line =~ /^\s*HADOOP_COMMON_HOME=/);
						$line = "HADOOP_HDFS_HOME=\"/grid/4/hadoop-hdfs\" # $line" if ($line =~ /^\s*HADOOP_HDFS_HOME=/);
						$line = "HADOOP_MAPRED_HOME=\"/grid/4/hadoop-mapreduce\" # $line" if ($line =~ /^\s*HADOOP_MAPRED_HOME=/);
						$line = "HADOOP_YARN_HOME=\"/grid/4/hadoop-yarn\" # $line" if ($line =~ /^\s*HADOOP_YARN_HOME=/);
					}
				}
				if (open(LFHOUT, ">$hadoop_layout")){
				    print LFHOUT join("",@lines),"\n";
				    close(LFHOUT);
				} else {
				    print STDERR "ERROR - Failed to open file $hadoop_layout for writting : $!\n"; 
					$errors++; 
				}
			}else { 
				print STDERR "ERROR - Failed to open file $hadoop_layout for reading : $!\n"; 
				$errors++; 
			}
			if (open(CFHIN, "<$hadoop_config")){
				my @lines = <CFHIN>;
				close(CFHIN);
				foreach my $line (@lines){
                                    next unless(defined($line));
                                    if ($line =~ /\s*if\s*\[ -d.*\/usr\/hdp\/$hdp_version\/tez.*\].*$/){
                                        $line = 'TEZ_HOME=${TEZ_HOME:-"/grid/4/tez"}' . "\n" . 'if [ -d "$TEZ_HOME" ]; then' . "\n\t";
					$line .= 'export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$TEZ_HOME/*:$TEZ_HOME/lib/* #:/etc/tez/conf'."\n\t";
                                        $line .= 'export CLASSPATH=${CLASSPATH}:$TEZ_HOME/*:$TEZ_HOME/lib/* #:/etc/tez/conf' ."\n";
                                    }
                                    $line="" if ($line =~ /\s*export HADOOP_CLASSPATH=.*\/usr\/hdp\/$hdp_version\/tez.*$/);
                                    $line="" if ($line =~ /\s*export CLASSPATH=.*\/usr\/hdp\/$hdp_version\/tez.*$/);
                                    $line="# $line" if ($line =~ /\s*\[\s*-f\s*\"\/usr\/hdp\/$hdp_version\/hadoop\/conf\/set-hdfs-plugin-env.sh\"\s*\].*$/); 
                                }
				if (open(CFHOUT, ">$hadoop_config")){
				    print CFHOUT join("",@lines),"\n";
				    close(CFHOUT); 
				} else {
					print STDERR "ERROR - Failed to open file $hadoop_config for writting : $!\n";  
					$errors++; 
				}
			}else {
				print STDERR "ERROR - Failed to open file $hadoop_config for reading : $!\n"; 
				$errors++; 
			}
                        my $cmd1 = "/bin/cp -pR $hadoop_home/libexec $hadoop_home/libexec1";
                        my $e_code = system($cmd1);
                        if (determine_success($e_code,FALSE)){
                            my $hadoop_layout1 = "$hadoop_home/libexec1/hadoop-layout.sh";
                            my $hadoop_config1 = "$hadoop_home/libexec1/hadoop-config.sh";
                            if (open(LFHIN1,"<$hadoop_layout1")){
                                 my @lines1 = <LFHIN1>;
                                 close(LFHIN1);
                                 foreach my $line1 (@lines1){
                                     if(defined($line1)){
                                       if ($line1 =~ /^\s*HADOOP_LIBEXEC_DIR=/){
                                             $line1 = "HADOOP_LIBEXEC_DIR=\${HADOOP_LIBEXEC_DIR:-\"/grid/4/hadoop/libexec1\"}\n";
                                             last;
                                       }
                                     }
                                 }
                                 if (open(LFHOUT1, ">$hadoop_layout1")){
                                       print LFHOUT1 join("",@lines1),"\n";
                                       close(LFHOUT1);
                                 }else{
                                     print STDERR "ERROR - Failed to open file $hadoop_layout1 for writting : $!\n";
                                 }
                            }else{
                                print STDERR "ERROR - Failed to open file $hadoop_layout1 for reading : $!\n";
                            }
                            if (open(CFHIN1, "<$hadoop_config1")){
                                my @lines1 = <CFHIN1>;
                                close(CFHIN1);
                                my $found = FALSE;
                                foreach my $line1 (@lines1){
                                     next unless(defined($line1));
                                     $line1 = "" if ($line1 =~ /\s*TEZ_HOME=\$\{TEZ_HOME:-\"\/grid\/4\/tez\"\}.*$/ || $line1 =~ /\s*TEZ_HOME=\$\{TEZ_HOME:-\/grid\/4\/tez\}.*$/i);
                                     if ($line1 =~ /\s*if \[ -d \"\$TEZ_HOME\" \].*$/ || $line1 =~ /\s*export HADOOP_CLASSPATH=\$\{HADOOP_CLASSPATH\}:\$TEZ_HOME.*$/){
                                           $line1 = "";
                                           $found = TRUE;
                                     }
                                     $line1 = "" if ($line1 =~ /\s*export CLASSPATH=\$\{CLASSPATH\}:\$TEZ_HOME.*$/);
                                     $line1 = "" if ($found && $line1 =~ /\s*fi\s*.*$/);
                                }
                                if (open(CFHOUT1, ">$hadoop_config1")){
                                    print CFHOUT1 join("",@lines1),"\n";
                                    close(CFHOUT1);
                                }else{
                                    print STDERR "ERROR - Failed to open file $hadoop_config1 for writting : $!\n";
                                }
                            }else{ 
                                print STDERR "ERROR - Failed to open file $hadoop_config1 for reading : $!\n"; 
                            }
                        }
		} else { $errors++; }
		$cmd = "mv $hadoop_hdfs_home/bin/hdfs $hadoop_hdfs_home/bin/hdfs.ori; cp $hadoop_hdfs_home/bin/hdfs.distro $hadoop_hdfs_home/bin/hdfs; rm -rf $hadoop_hdfs_home/etc; ls -lh $hadoop_hdfs_home/bin; ls -lh $hadoop_hdfs_home ";
		system($cmd);
		$cmd = "mv $hadoop_mr_home/bin/mapred $hadoop_mr_home/bin/mapred.ori; cp $hadoop_mr_home/bin/mapred.distro $hadoop_mr_home/bin/mapred; rm -rf $hadoop_mr_home/etc; ls -lh $hadoop_mr_home/bin; ls -lh hadoop_mr_home";
		system($cmd);
		$cmd = "mv $hadoop_yarn_home/bin/yarn $hadoop_yarn_home/bin/yarn.ori; cp $hadoop_yarn_home/bin/yarn.distro $hadoop_yarn_home/bin/yarn; rm -rf $hadoop_yarn_home/etc;";
		$cmd .= "mv $hadoop_yarn_home/bin/mapred $hadoop_yarn_home/bin/mapred.ori; cp mv $hadoop_yarn_home/bin/maped.distro mv $hadoop_yarn_home/bin/mapred; ls -lh $hadoop_yarn_home/bin; ls -lh $hadoop_yarn_home";
		system($cmd);
		$cmd = "unlink $tez_home/conf; ls -lh $tez_home";
		system($cmd);
	} else {$errors++; }
	chdir($cur_dir);
	return TRUE if ($errors <= 0);
	return FALSE;
	
}

sub setup_hive_for_tarball(){
    my $hive_home = "$hdp_dir/hive";
    my $cmd = "unlink $hive_home/conf; rm -rf $hive_home/etc; ";
    system($cmd);
    my $errors = 0;
    if (chdir("$hive_home/bin")){
            $cmd = "cd $hive_home/bin; pwd;";
	    $cmd .= 'for f in *.distro; do f1=${f/.distro};  mv $f1 $f1.ori; /bin/cp $f $f1;  done';
            print $cmd,"\n";
	    my $exit_code = system($cmd);
            system("ls -lh ; ls -lh ../");
	    $errors++ unless (determine_success($exit_code, TRUE));
	}else { $errors++; }
	if (chdir("$hive_home/lib")){
                $cmd = "cd $hive_home/lib; pwd;";
		$cmd .= "wget http://repo1.maven.org/maven2/mysql/mysql-connector-java/5.1.29/mysql-connector-java-5.1.29.jar";
                print $cmd,"\n";
		my $exit_code = system($cmd);
		system("pwd ; ls -lh mysql-connector-java-5.1.29.jar");
		$errors++ unless (determine_success($exit_code,TRUE));
	} else { $errors++;}	
	chdir($cur_dir);
        return TRUE if ($errors <= 0);
        return FALSE;
}

sub setup_spark_for_tarball(){
        my $spark_home = "$hdp_dir/spark";
        my $cmd = "rm -fr $spark_home/etc $spark_home/logs $spark_home/work $spark_home/conf;";
        $cmd .= " mkdir $spark_home/logs $spark_home/work; chmod 1777 $spark_home/logs $spark_home/works; ls -lh $spark_home";
        system($cmd);
        my $spark_class_file_path = "$spark_home/bin/spark-class";
        $cmd = "/bin/cp -fv $spark_class_file_path  ${spark_class_file_path}.orig";
        my $exit_code1 = system($cmd);
        if (determine_success($exit_code1, TRUE)){
            if(open(SCFPIN, "<$spark_class_file_path")){
                my @lines = <SCFPIN>;
                close(SCFPIN);
                foreach my $line(@lines){
                    next unless($line);
                    if ($line =~ /^\s*HADOOP_LZO_DIR=\"\/usr\/hdp\/\$\{HDP_VERSION\}\/hadoop\/lib\".*$/){
                         $line = 'HADOOP_LZO_DIR=${HADOOP_LZO_DIR:-"/grid/4/hadoop/lib"} #'. $line;
                         last;
                    }
                }
                if(open(SCFPOUT, ">$spark_class_file_path")){
                    print SCFPOUT join("",@lines),"\n";
                    close(SCFPOUT);
                }else{
                    print STDERR "ERROR - Failed to open file $spark_class_file_path for writting : $!\n";
                }
            }else{
                print STDERR "ERROR - Failed to open file $spark_class_file_path for reading : $!\n";
            }
        }
        my $spark_hdp_assembly_jar = "spark-hdp-assembly.jar";
        my @spark_assembly_jars = glob("$spark_home/lib/spark-assembly*2.4.2.0-12*hadoop*2.4.2.0-12.jar");
        print "Spark Assembly Jar: ", join(" ",@spark_assembly_jars), "\n";
        if(scalar(@spark_assembly_jars) > 0 && $spark_assembly_jars[0] && $spark_assembly_jars[0] =~ /^.*spark-assembly.*$hdp_version.*hadoop.*$hdp_version.jar\s*$/){
              my $spark_assembly_jar = basename($spark_assembly_jars[0]);
              $cmd = "if cd $spark_home/lib;then pwd; unlink $spark_hdp_assembly_jar; ln -s $spark_assembly_jar $spark_hdp_assembly_jar; ls -l;fi";
              system($cmd);
        } 
}

sub setup_oozie_for_tarball(){
	my $oozie_home = "$hdp_dir/oozie";
	my $errors=0;
	my $cmd = "rm -rf $oozie_home/etc; unlink $oozie_home/conf; /bin/cp -v $oozie_home/libserver/derby*.jar $oozie_home/libext/;";
	$cmd .= "/bin/cp -v $download_dir/usr/share/HDP-oozie/ext-2.2.zip $oozie_home/libext/";
	system($cmd);
	if (chdir("$oozie_home/libext")){
		$cmd = "cd $oozie_home/libext; pwd; wget http://repo1.maven.org/maven2/mysql/mysql-connector-java/5.1.29/mysql-connector-java-5.1.29.jar";
		my $exit_code = system($cmd);
		system("pwd ; ls -lh mysql-connector-java-5.1.29.jar");
		$errors++ unless (determine_success($exit_code, TRUE));
	}else{ $errors++;}
	if (chdir("$oozie_home/oozie-server")){
		$cmd = "cd $oozie_home/oozie-server;unlink conf;pwd";
		system($cmd);
		my $cmd = "cd $oozie_home/oozie-server; pwd;ln -s ../tomcat-deployment/conf conf";
		my $exit_code = system($cmd);
		$errors++ unless (determine_success($exit_code,TRUE));
	}else{ $errors++;}
	if (chdir("$oozie_home/bin")){
		$cmd = "cd $oozie_home/bin; pwd; mv $oozie_home/bin/oozie $oozie_home/bin/oozie.ori; mv $oozie_home/bin/oozied.sh $oozie_home/bin/oozied.sh.ori";
		system($cmd);
		$cmd = "cd $oozie_home/bin; pwd; /bin/cp -v $oozie_home/bin/oozie.distro $oozie_home/bin/oozie";
		my $exit_code = system($cmd);
		$cmd = "cd $oozie_home/bin; pwd; /bin/cp -v $oozie_home/bin/oozied.distro $oozie_home/bin/oozied.sh";
		my $exit_code1 = system($cmd);
		$errors++ unless (determine_success($exit_code,TRUE));
		$errors++ unless (determine_success($exit_code1, TRUE));
	}
	chdir($cur_dir);
	return TRUE if ($errors <= 0);
	return FALSE;
}

sub prepare_tarball(){
	if (chdir($hdp_dir_base)){
		my $cmd = "cd $hdp_dir_base; pwd; tar cvzf $tarball_dir/hdp-${hdp_version}.tar.gz $hdp_version";
                print $cmd, "\n";
		my $exit_code = system($cmd);
                print "$tarball_dir/hdp-${hdp_version}.tar.gz\n";
		return determine_success($exit_code,TRUE);
	}
	return FALSE;
}

sub help_msg{
    my $exit_status = shift @_ || 0;
    print STDERR "Usage $0: \n$0 Options\n\t";
    print STDERR " -v | --hdp-version=<Required: hdp-version-string>\n\t";
    print STDERR " -d | --download-dir=<Required: directory where for downloading rpms>, Directory must be writable\n\t";
    print STDERR " -t | --tarball_dir=<Optional: directory for placing resultant tarball, default is same as --download-dir>] Direcory must be writable\n\t";
    print STDERR " -h | --help \n";
    exit($exit_status);
}
sub main(){
	GetOptions('hdp-version|v=s' => \$hdp_version,'download-dir|d=s' => \$download_dir, 'tarball_dir|t=s' => \$tarball_dir, 
         "help|h" => sub {help_msg(0)}) or die("Error in command line arguments\n");
	unless ($hdp_version && $hdp_version =~ /\d+\.\d+\.\d+\.\d+-\d+/ && $download_dir && -d $download_dir && -w $download_dir){
                 help_msg(1);            
	}
        $tarball_dir = $download_dir unless ($tarball_dir);
        $hdp_dir_base = "$download_dir/usr/hdp";
        $hdp_dir = "$download_dir/usr/hdp/$hdp_version";
	$hdp_version_subs = $hdp_version;
	$hdp_version_subs =~ s/\./_/g;
	download_repo;
	download_tarballs;
	setup_hadoop_for_tarball;
	setup_hive_for_tarball;
        setup_spark_for_tarball;
	setup_oozie_for_tarball;
        #print $hdp_dir_base,"\n";
	prepare_tarball;
}

main;

