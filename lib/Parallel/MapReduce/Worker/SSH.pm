package Parallel::MapReduce::Worker::SSH;

use strict;
use warnings;

use base 'Parallel::MapReduce::Worker';

use Data::Dumper;
use IPC::Run qw(start pump finish timeout);
use Parallel::MapReduce::Worker;

=pod

=head1 NAME

Parallel::MapReduce::Worker::SSH - MapReduce, remote worker via SSH

=head1 SYNOPSIS

  use Parallel::MapReduce::Worker::SSH;
  my $w = new Parallel::MapReduce::Worker::SSH (host => '10.0.10.2');

  # otherwise same interface as parent class Parallel::MapReduce::Worker

=head1 DESCRIPTION

This subclass of L<Parallel::MapReduce::Worker> implements a remote worker using SSH for launching
and the resulting SSH tunnel for communicating.

By default, the package is trying an SSH client C</usr/bin/ssh> and is assuming that the Perl binary
on the remote machine is C</usr/bin/perl>. Tweak the package variables C<$SSH> and C<$PERL> if these
assumptions are wrong.

=cut

our $SSH  = '/usr/bin/ssh';
our $PERL = '/usr/bin/perl';

=pod

=head1 INTERFACE

=head2 Constructor

The construct expects the following fields:

=over

=item C<host> (default: none)

At constructor time an SSH connection to the named host is attempted. Then a remote Perl program to
implement the worker there is started. For this, obviously C<Parallel::MapReduce> must be installed
on the remote machine.

=back

B<NOTE>: Do not forget to call C<shutdown> on an SSH worker, otherwise you will have a lot of
lingering SSH connections.

=cut

sub new {
    my $class = shift;
    my %opts  = @_;
    my $self = bless { host => $opts{host},
		       in   => '',
		       out  => '',
		       err  => '',
		   }, $class;
warn "starting up ".$self->{host};
    $self->{harness} = start [ split /\s+/, "$SSH ".$self->{host}." $PERL -I/home/rho/projects/mapreduce/lib -MParallel::MapReduce::Worker::SSH -e 'Parallel::MapReduce::Worker::SSH::ssh_worker()'" ], 
                             \ $self->{in}, \ $self->{out}, \ $self->{err},
                             timeout( 20 ) ;
 warn "started up ".$self->{host};
    return $self;
}

sub shutdown {
    my $self = shift;

    $self->{in} .= "exit\n";
    pump $self->{harness};	      # make sure the worker gets exit
    $self->{harness}->finish;           # make sure the worker is dead
}


sub map {
    my $self = shift;
    my $cs = shift;
    my $sl = shift;
    my $ss = shift;
    my $jj = shift;

    $self->{in} = $self->{out} = $self->{err} = '';
    $self->{in} .= "mapper\n";
    $self->{in} .= "$jj\n";
    $self->{in} .= "$sl\n";
    $self->{in} .= join (",",  @$ss ) . "\n";
    $self->{in} .= join ("\n", @$cs ) . "\n\n";
warn "sent chunks: ".Dumper $cs;;

    pump $self->{harness} until $self->{out} =~ /\n\n/g;
warn "ssh worker sent back err".$self->{err};
warn "ssh worker sent back out".$self->{out};

    return [ split /\n/, $self->{out} ];
}

sub reduce {
    my $self = shift;
    my $ks = shift;
    my $ss = shift;
    my $jj = shift;
warn "master writes to reduce ".Dumper ($ks, $ss, $jj);

    $self->{in} = $self->{out} = $self->{err} = '';
    $self->{in} .= "reducer\n";
    $self->{in} .= "$jj\n";
    $self->{in} .= join (",",  @$ss ) . "\n";
    $self->{in} .= join ("\n", @$ks ) . "\n\n";

warn "sent ".scalar @$ks." keys";

    pump $self->{harness} until $self->{out} =~ /\n\n/g;
warn "ssh worker sent back err".$self->{err};
warn "ssh worker sent back out".$self->{out};

###    $self->{task} = undef;
    return [ split /\n/, $self->{out} ];
}

sub _pull_string_stdin {
    my $s = <STDIN>; chomp $s;
    return $s;
}

sub _pull_hlist_stdin {
    my $s = <STDIN>; chomp $s;
    return [ split /,/, $s ];
}

sub _pull_vlist_stdin {
    my $s;
    my @s;
    do {
	$s = <STDIN>; chomp $s;
	push @s, $s if $s;
    } while ($s);
    return \@s;
}

sub ssh_worker {
    use IO::Handle;
    STDOUT->autoflush(1);
    STDERR->autoflush(1);

    use constant COWS_COME_HOME => 0;

    do {
	my $mode = _pull_string_stdin();
	exit if $mode eq 'exit';
	if ($mode eq "mapper") {
	    my $job     = _pull_string_stdin();
	    my $slice   = _pull_string_stdin();
	    my $servers = _pull_hlist_stdin();
	    my $chunks  = _pull_vlist_stdin();
	    warn "gotta $job $slice servers ".scalar @$servers. "chunks: ".scalar @$chunks;

	    my $w  = new Parallel::MapReduce::Worker;
	    my $cs = $w->map ($chunks, $slice, $servers, $job);
	    print join ("\n", @$cs) . "\n\n";

	} elsif ($mode eq "reducer") {
	    my $job     = _pull_string_stdin();
	    my $servers = _pull_hlist_stdin();
	    my $keys    = _pull_vlist_stdin();
	    warn "reducer gotta $job servers ".scalar @$servers. "keys: ".scalar @$keys;

	    my $w  = new Parallel::MapReduce::Worker;
	    my $cs = $w->reduce ($keys, $servers, $job);
	    print join ("\n", @$cs) . "\n\n";
	}
	sleep 2;
    } until (COWS_COME_HOME);
}

=pod

=head1 SEE ALSO

L<Parallel::MapReduce::Worker>

=head1 COPYRIGHT AND LICENSE

Copyright 200[8] by Robert Barta, E<lt>drrho@cpan.orgE<gt>

This library is free software; you can redistribute it and/or modify it under the same terms as Perl
itself.

=cut

our $VERSION = 0.04;

1;

__END__

sub __ssh_remoted_map_worker {
    my $self = shift;
    my ($chunks, $slice, $servers, $job) = @{ $comm };
    my $cs = $self->SUPER::map_worker ($chunks, $slice, $servers, $job);
    $comm = $cs;
}

sub __ssh_remoted_reduce_worker {
    my $self = shift;
    my ($keys, $servers, $job) = @{ $comm };
    my $cs = $self->SUPER::reduce_worker ($keys, $servers, $job);
    $comm = $cs;
}

