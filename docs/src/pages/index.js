import React from 'react';
import Layout from '@theme/Layout';
import clsx from 'clsx';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Container from '../core/Container';
import GridBlock from '../core/GridBlock';
import useBaseUrl from '@docusaurus/useBaseUrl';

const Hero = () => {
  const { siteConfig } = useDocusaurusContext();
  return (
    <div className="homeHero">
      <div className="logo"><img src={useBaseUrl('img/pattern.svg')} /></div>
      <div className="container banner">
        <div className="row">
          <div className={clsx('col col--5')}>
            <div className="homeTitle">{siteConfig.tagline}</div>
            <small className="homeSubTitle">Meteor is an open source plugin-driven metadata collection framework to extract data from different sources and sink to any data catalog or store.</small>
            <a className="button" href="docs/introduction">Documentation</a>
          </div>
          <div className={clsx('col col--1')}></div>
          <div className={clsx('col col--6')}>
            <div className="text--right"><img src={useBaseUrl('img/banner.svg')} /></div>
          </div>
        </div>
      </div>
    </div >
  );
};

export default function Home() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout
      title={siteConfig.tagline}
      description="Meteor is an easy-to-use, plugin-driven metadata collection framework to extract data from different sources and sink to any data catalog or store.">
      <Hero />
      <main>
        <Container className="textSection wrapper" background="light">
          <h1>Built for ease</h1>
          <p>
            Meteor is a plugin driven agent for collecting metadata.
            Meteor has plugins to source metadata from a variety of data stores,
            services and message queues. It also has sink plugins to send metadata
            to variety of third party APIs and catalog services.
          </p>
          <GridBlock
            layout="threeColumn"
            contents={[
              {
                title: 'Zero dependency',
                content: (
                  <div>
                    Meteor is written in Go and compiles into a single binary with no external dependencies,
                    and requires a very minimal memory footprint.
                  </div>
                ),
              },
              {
                title: 'Coverage',
                content: (
                  <div>
                    With 50+ plugins and many more coming soon to extract and sink metadata, it is easy
                    to start collecting metadata from various sources.
                  </div>
                ),
              },
              {
                title: 'Extensible',
                content: (
                  <div>
                    With the ease of plugin development you can build your own plugin to fit with
                    your needs. It allows new sources, processors and sinks to be easily added.
                  </div>
                ),
              },
              {
                title: 'CLI',
                content: (
                  <div>
                    Meteor comes with a CLI which allows you to interact with agent effectively.
                    You can list all plugins, start and stop agent, and more.
                  </div>
                ),
              },
              {
                title: 'Proven',
                content: (
                  <div>
                    Battle tested at large scale across multiple companies. Largest deployment collect
                    metadata from thousands of data sources.
                  </div>
                ),
              },
              {
                title: 'Runtime',
                content: (
                  <div>
                    Meteor can run from your local machine, cloud server machine or containers with minium
                    efforts required for deployment.
                  </div>
                ),
              },
            ]}
          />
        </Container>
        <Container className="textSection wrapper" background="dark">
          <h1>Framework</h1>
          <p>
            Meteor agent uses recipes as a set of instructions which are configured by user.
            Recipes contains configurations about the source from which the metadata will be
            fetched, information about metadata processors and the destination to where
            the metadata will be sent.
          </p>
          <GridBlock
            layout="threeColumn"
            contents={[
              {
                title: 'Extraction',
                content: (
                  <div>
                    Extraction is the process of extracting data from a source and
                    transforming it into a format that can be consumed by the agent.
                    Extractors are the set of plugins that are source of our
                    metadata and include databases, dashboards, users, etc.
                  </div>
                ),
              },
              {
                title: 'Processing',
                content: (
                  <div>
                    Processing is the process of transforming the extracted data
                    into a format that can be consumed by the agent.
                    Processors are the set of plugins that perform the enrichment
                    or data processing for the metadata after extraction..
                  </div>
                ),
              },
              {
                title: 'Sink',
                content: (
                  <div>
                    Sink is the process of sending the processed data to a single or
                    multiple destinations as defined in recipes.
                    Sinks are the set of plugins that act as the destination of our metadata
                    after extraction and processing is done by agent.
                  </div>
                ),
              },
            ]}
          />
        </Container>


        <Container className="textSection wrapper" background="light">
          <h1>Ecosystem</h1>
          <p>
            Meteor’s plugin system allows new plugins to be easily added.
            With 50+ plugins and many more coming soon to extract and sink metadata,
            it is easy to start collecting metadata from various sources and
            sink to any data catalog or store.
          </p>
          <div className="row">
            <div className="col col--4">

              <GridBlock
                contents={[
                  {
                    title: 'Extractors',
                    content: (
                      <div>
                        Meteor supports source plugins to extract metadata from a variety of
                        datastores services, and message queues, including BigQuery,
                        InfluxDB, Kafka, Metabase, and many others.
                      </div>
                    ),
                  },
                  {
                    title: 'Processors',
                    content: (
                      <div>
                        Meteor has in-built processors including enrichment and others.
                        It is easy to add your own processors as well using custom plugins.
                      </div>
                    ),
                  },
                  {
                    title: 'Sinks',
                    content: (
                      <div>
                        Meteor supports sink plugins to send metadata to a variety of
                        third party APIs and catalog services, including Compass, HTTP, BigQuery,
                        Kafka, and many others.
                      </div>
                    ),
                  },
                ]}
              />
            </div>
            <div className="col col--8">
              <img src={useBaseUrl('assets/overview.svg')} />
            </div>
          </div>
        </Container>

        {/* <Container className="textSection wrapper" background="light">
          <h1>Trusted by</h1>
          <p>
            Meteor was originally created for the Gojek data processing platform,
            and it has been used, adapted and improved by other teams internally and externally.
          </p>
          <GridBlock className="logos"
            layout="fourColumn"
            contents={[
              {
                content: (
                  <img src={useBaseUrl('users/gojek.png')} />
                ),
              },
              {
                content: (
                  <img src={useBaseUrl('users/midtrans.png')} />
                ),
              },
              {
                content: (
                  <img src={useBaseUrl('users/mapan.png')} />
                ),
              },
              {
                content: (
                  <img src={useBaseUrl('users/moka.png')} />
                ),
              },
            ]}>
          </GridBlock>
        </Container> */}
      </main>
    </Layout >
  );
}
