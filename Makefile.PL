use 5.008000;
use ExtUtils::MakeMaker;

WriteMakefile(
	      NAME              => 'Parallel::MapReduce',
	      VERSION_FROM      => 'lib/Parallel/MapReduce.pm',
	      PREREQ_PM         => {
		  'Cache::Memcached' => 1.24,
		  'IPC::Run'         => 0.80,
		  'Test::More'       => 0.72,
	      },
	      ABSTRACT_FROM  => 'lib/Parallel/MapReduce.pm',
	      AUTHOR         => 'Robert Barta <drrho@cpan.org>',
);


package MY;

sub depend {
    return <<MAKE

upload:
	cpan-upload-http Parallel-MapReduce-\$(VERSION).tar.gz

dependencies:
	perl support/dependencies.pl

prepare_test:
	export MR_ONLINE=1

MAKE
}
